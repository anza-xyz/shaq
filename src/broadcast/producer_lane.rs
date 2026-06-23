//! One producer's lane: a self-contained shared-memory block holding the lane's
//! ownership state, its reserve/publication cursors, the per-consumer reserve
//! limits, and the ring of payloads. See [`ProducerLane`].

use core::mem::MaybeUninit;
use core::mem::{align_of, size_of};
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use core::slice;
use core::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};

use crate::CacheAlignedAtomicSize;

const LANE_FREE: u64 = 0;
const LANE_ACTIVE: u64 = 1;

/// Value stored in a consumer slot that no consumer owns.
///
/// A real reserve limit (see the consumer-slot docs below) never reaches
/// `usize::MAX` — that would take millennia at any real publish rate — so this
/// is an unambiguous "no constraint" sentinel: it sits at the top, so it drops
/// out of the producer's `min` and never gates a reserve.
const UNCLAIMED: usize = usize::MAX;

/// Fixed-size head of a producer-lane block.
#[repr(C)]
struct LaneHeader {
    /// Lane ownership: `LANE_FREE` or `LANE_ACTIVE`.
    state: AtomicU64,
    /// Claimed-up-to sequence: advanced before a ring cell is written.
    producer_reservation: CacheAlignedAtomicSize,
    /// Visible-up-to sequence: advanced after a ring cell is written; consumers
    /// read sequences `< producer_publication`.
    producer_publication: CacheAlignedAtomicSize,
}

/// A single producer's lane.
///
/// The lane holds: ownership state, the reserve/publication cursors, one
/// consumer slot per consumer (its read progress, doubling as its hazard),
/// and the ring of `T`. The owning producer mutates the ring and cursors
/// (`&mut self`); consumers read published payloads and publish their own
/// progress (`&self`).
///
/// ## Consumer slot = reserve limit (`next_to_read + capacity`)
///
/// A consumer slot does **not** store the consumer's raw read cursor. It stores
/// that consumer's **reserve limit**: `next_to_read + capacity`, i.e. the lowest
/// sequence the producer may NOT yet reserve to avoid overwriting an active read.
/// An unowned slot holds [`UNCLAIMED`].
///
/// Block layout: `LaneHeader`, then `[CacheAlignedAtomicSize; consumer_slots]`
/// limits (one per cache line), then `[T; capacity]`.
pub(crate) struct ProducerLane<T> {
    header: NonNull<LaneHeader>,
    consumers: NonNull<CacheAlignedAtomicSize>,
    ring: NonNull<T>,

    mask: usize, // capacity - 1
    consumer_slots: usize,
}

#[inline]
const fn consumers_offset() -> usize {
    size_of::<LaneHeader>().next_multiple_of(align_of::<CacheAlignedAtomicSize>())
}

#[inline]
fn ring_offset<T>(consumer_slots: usize) -> Option<usize> {
    consumers_offset()
        .checked_add(consumer_slots.checked_mul(size_of::<CacheAlignedAtomicSize>())?)?
        .checked_next_multiple_of(align_of::<T>())
}

impl<T> ProducerLane<T> {
    /// Bytes needed for one lane block of `capacity` payload cells and
    /// `consumer_slots` consumer slots.
    pub(crate) fn block_size(capacity: u32, consumer_slots: usize) -> Option<usize> {
        ring_offset::<T>(consumer_slots)?
            .checked_add((capacity as usize).checked_mul(size_of::<T>())?)
    }

    /// Required alignment of a lane block: the `LaneHeader`'s alignment.
    ///
    /// The block starts with the `LaneHeader`; the trailing `[T]` ring is aligned
    /// by its offset within the block (`ring_offset` is a multiple of
    /// `align_of::<T>()`), so the block itself only needs the header's alignment —
    /// as long as `T`'s alignment divides it, which holds for any normal payload
    /// (alignment ≤ 64). Asserted so an over-aligned `T` is a clear compile error.
    pub(crate) const fn block_align() -> usize {
        const {
            assert!(
                align_of::<T>() <= align_of::<LaneHeader>(),
                "payload alignment exceeds the lane header alignment",
            );
        }
        align_of::<LaneHeader>()
    }

    /// Initializes a lane block: ownership/cursors zeroed, every consumer slot
    /// free. The ring is left uninitialized (each cell is written before it is
    /// published, and read only after).
    ///
    /// # Safety
    /// - `block` must point at a [`Self::block_size`] region for `(capacity,
    ///   consumer_slots)` and be initialized at most once.
    pub(crate) unsafe fn init(block: NonNull<u8>, consumer_slots: usize) {
        // SAFETY: `block` begins with a `LaneHeader`.
        unsafe {
            block.cast::<LaneHeader>().as_ptr().write(LaneHeader {
                state: AtomicU64::new(LANE_FREE),
                producer_reservation: CacheAlignedAtomicSize::default(),
                producer_publication: CacheAlignedAtomicSize::default(),
            });
        }
        // SAFETY: layout reserves `consumer_slots` aligned slots here; init runs
        // once with no other handle joined, so &mut is exclusive.
        let slots: &mut [MaybeUninit<CacheAlignedAtomicSize>] = unsafe {
            slice::from_raw_parts_mut(
                block
                    .byte_add(consumers_offset())
                    .cast::<MaybeUninit<CacheAlignedAtomicSize>>()
                    .as_ptr(),
                consumer_slots,
            )
        };
        for slot in slots {
            slot.write(CacheAlignedAtomicSize {
                inner: AtomicUsize::new(UNCLAIMED),
            });
        }
    }

    /// Builds a lane view over an initialized block.
    ///
    /// # Safety
    /// - `block` must reference a block initialized by [`Self::init`] with the
    ///   same `consumer_slots`, sized for `capacity`, alive for the view's use.
    pub(crate) unsafe fn from_block(
        block: NonNull<u8>,
        capacity: u32,
        consumer_slots: usize,
    ) -> Self {
        debug_assert!(capacity.is_power_of_two());

        let header = block.cast();
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for consumers to fit (if init succeeded).
        let consumers = unsafe { block.byte_add(consumers_offset()) }.cast();
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for ring data to fit (if init succeeded).
        let ring = unsafe {
            block.byte_add(ring_offset::<T>(consumer_slots).expect("validated_lane layout"))
        }
        .cast();

        Self {
            header,
            consumers,
            ring,
            mask: (capacity as usize).wrapping_sub(1),
            consumer_slots,
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.mask.wrapping_add(1)
    }

    #[inline]
    fn header(&self) -> &LaneHeader {
        // SAFETY: The lane has an initialized header that lives for the lane's lifetime.
        unsafe { self.header.as_ref() }
    }

    /// The slot holding consumer `consumer_index`'s reserve limit
    /// (`next_to_read + capacity`, or [`UNCLAIMED`]).
    #[inline]
    fn consumer_limit(&self, consumer_index: usize) -> &CacheAlignedAtomicSize {
        debug_assert!(consumer_index < self.consumer_slots);
        // SAFETY: `consumer_index < consumer_slots`; the slot was initialized.
        unsafe { &*self.consumers.add(consumer_index).as_ptr() }
    }

    /// Pointer to the ring cell holding `sequence` — used by the producer to
    /// write a reserved cell and by consumers to read a published one.
    #[inline]
    pub(crate) fn payload_ptr(&self, sequence: usize) -> NonNull<T> {
        // SAFETY: `sequence & mask < capacity`; the ring has `capacity` cells.
        unsafe { self.ring.add(sequence & self.mask) }
    }

    /// Claims the lane for a producer. Returns `false` if already owned.
    pub(crate) fn try_acquire(&self) -> bool {
        self.header()
            .state
            .compare_exchange(LANE_FREE, LANE_ACTIVE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Releases the lane back to free.
    pub(crate) fn release(&self) {
        let _ = self.header().state.compare_exchange(
            LANE_ACTIVE,
            LANE_FREE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Rewinds the reservation back to the publication. A dead producer may have
    /// reserved cells it never published; those were never visible to a consumer,
    /// so they are safely discarded, restoring the `reservation == publication`
    /// invariant the next owner continues from.
    fn rewind_reservation(&self) {
        let header = self.header();
        let published = header.producer_publication.load(Ordering::Acquire);
        header
            .producer_reservation
            .store(published, Ordering::Release);
    }

    /// Force-claims a lane whose previous producer died and rewinds its
    /// reservation, so the recovered producer resumes publishing from the last
    /// published sequence.
    ///
    /// The caller must guarantee (via the global ownership contract) that the
    /// previous owner is dead and no other producer holds this lane.
    pub(crate) fn recover(&self) {
        self.header().state.store(LANE_ACTIVE, Ordering::Release);
        self.rewind_reservation();
    }

    /// Force-releases a dead producer's lane back to free (rewinding its
    /// reservation first) so the normal acquire path can reclaim it.
    pub(crate) fn force_release(&self) {
        self.rewind_reservation();
        self.header().state.store(LANE_FREE, Ordering::Release);
    }

    /// The binding reserve limit across consumers: the lowest sequence the
    /// producer may NOT yet reserve. Each slot holds `next_to_read + capacity`;
    /// unowned slots hold [`UNCLAIMED`] and so drop out of the `min`. With every
    /// slot unowned the result is [`UNCLAIMED`] (no constraint).
    fn reserve_limit(&self) -> usize {
        let mut limit = UNCLAIMED;
        for consumer_index in 0..self.consumer_slots {
            let consumer_limit = self.consumer_limit(consumer_index).load(Ordering::Acquire);
            if consumer_limit < limit {
                limit = consumer_limit;
            }
        }
        limit
    }

    /// Reserves `count` consecutive sequences for writing, returning the first.
    /// `None` on backpressure: the batch would overwrite a cell an active
    /// consumer has not yet read, or it exceeds the ring capacity.
    ///
    /// Write each reserved cell via [`Self::payload_ptr`], then [`Self::publish`].
    pub(crate) fn reserve(&mut self, count: NonZeroUsize) -> Option<usize> {
        if count.get() > self.capacity() {
            return None;
        }
        let start = self.header().producer_reservation.load(Ordering::Acquire);
        // Producer half of the join handshake: order the previous reserve's
        // `producer_reservation` store before this reserve's limit loads, so a
        // racing `join_consumer` either sees our advance or we see its limit
        // (cf. `futex::Waiters`).
        fence(Ordering::SeqCst);
        // Each slot already stores `next_to_read + capacity`, so the gate is a
        // plain comparison: rejecting once the batch would reach a sequence a
        // consumer still needs. `UNCLAIMED` sits at the top, so unowned slots
        // (and the all-unowned case) never gate.
        if start.wrapping_add(count.get()) > self.reserve_limit() {
            return None;
        }
        // Claim before the writes; consumers only read `< producer_publication`.
        self.header()
            .producer_reservation
            .store(start.wrapping_add(count.get()), Ordering::Release);
        Some(start)
    }

    /// Publishes the next `count` reserved sequences, making them visible to
    /// consumers. Call after the cells are written.
    pub(crate) fn publish(&mut self, count: NonZeroUsize) {
        self.header()
            .producer_publication
            .fetch_add(count.get(), Ordering::Release);
    }

    /// Joins consumer `consumer_index` to this lane and returns the sequence it
    /// starts reading from.
    ///
    /// The caller must already own `consumer_index` through the broadcast's
    /// global consumer-ownership state, so this slot has a single writer (the
    /// owning consumer): no CAS is needed — a release store publishes the limit.
    ///
    /// The start is normally the lane's **current publication**: the next value
    /// to become visible after this join. With `from_backlog`, the start is
    /// instead up to one ring behind the publication (the oldest published item
    /// still guaranteed to be in the ring, floored at the first sequence), so a
    /// consumer on a slow queue can read data published before it joined. If the
    /// producer has already lapped that point, the handshake below fast-forwards
    /// to the reservation frontier — you cannot read cells already being
    /// recycled.
    ///
    /// The slot stores the **reserve limit** `start + capacity` (see the type
    /// docs). Reading the publication and publishing the limit are not atomic,
    /// so the producer can lap the start cell in the gap; the limit pins the
    /// producer once visible, so reading the reservation detects such a lap and
    /// fast-forwards to the now-pinned frontier.
    ///
    /// That lap detection is a StoreLoad handshake against the producer's
    /// `reserve`: a `SeqCst` fence here (between publishing the limit and
    /// re-reading the reservation) pairs with the matching fence in `reserve`,
    /// so the producer either sees our limit (and stops) or we see its advance
    /// (and fast-forward) — at least one (cf. `futex::Waiters`).
    ///
    /// The limit is published with a single release store, so the slot never
    /// passes through [`UNCLAIMED`]. This also makes the call usable for
    /// recovery: re-joining a dead owner's index overwrites its stale limit
    /// directly, never dropping this consumer's backpressure mid-claim.
    pub(crate) fn join_consumer(&self, consumer_index: usize, from_backlog: bool) -> usize {
        let capacity = self.capacity();
        let slot = self.consumer_limit(consumer_index);

        let start = {
            let published = self.published();
            if from_backlog {
                // Up to one ring behind the frontier (floored at the first
                // sequence): the oldest item still guaranteed to be in the ring.
                published.saturating_sub(capacity)
            } else {
                published
            }
        };
        let limit = start.wrapping_add(capacity);
        debug_assert!(limit != UNCLAIMED);
        slot.store(limit, Ordering::Release);

        // Consumer half of the join handshake (see the doc above).
        fence(Ordering::SeqCst);

        let reserved = self.reserved();
        if reserved > limit {
            slot.store(reserved.wrapping_add(capacity), Ordering::Release);
            return reserved;
        }
        start
    }

    /// Publishes consumer `consumer_index`'s progress: its next-to-read sequence
    /// `next_to_read`. The slot stores the reserve limit `next_to_read +
    /// capacity` (see the type docs) — the lowest sequence the producer may not
    /// yet overwrite for this consumer.
    pub(crate) fn set_consumer_cursor(&self, consumer_index: usize, next_to_read: usize) {
        let limit = next_to_read.wrapping_add(self.capacity());
        debug_assert!(limit != UNCLAIMED);
        self.consumer_limit(consumer_index)
            .store(limit, Ordering::Release);
    }

    /// Releases consumer slot `consumer_index` back to unowned (the reserve
    /// limit then ignores it).
    pub(crate) fn release_consumer(&self, consumer_index: usize) {
        self.consumer_limit(consumer_index)
            .store(UNCLAIMED, Ordering::Release);
    }

    /// Reconstructs a recovering consumer's next-to-read on this lane from its
    /// surviving reserve limit (`next_to_read + capacity`), so it resumes where
    /// the dead owner left off — those unread cells are still pinned by the
    /// limit. This only reads the slot, so this consumer's backpressure is never
    /// dropped. If the slot was never claimed (the owner died before joining this
    /// lane), start fresh at the frontier.
    pub(crate) fn recover_consumer(&self, consumer_index: usize) -> usize {
        let limit = self.consumer_limit(consumer_index).load(Ordering::Acquire);
        if limit == UNCLAIMED {
            self.join_consumer(consumer_index, false)
        } else {
            limit.wrapping_sub(self.capacity())
        }
    }

    #[inline]
    pub(crate) fn published(&self) -> usize {
        self.header().producer_publication.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn reserved(&self) -> usize {
        self.header().producer_reservation.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::Region;
    use std::num::NonZeroUsize;

    type Payload = u64;

    /// Allocates and initializes a standalone lane block.
    fn lane(
        capacity: u32,
        consumer_slots: usize,
    ) -> (std::sync::Arc<Region>, ProducerLane<Payload>) {
        let size = ProducerLane::<Payload>::block_size(capacity, consumer_slots).unwrap();
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
        // SAFETY: freshly allocated block, initialized once.
        unsafe { ProducerLane::<Payload>::init(region.addr(), consumer_slots) };
        // SAFETY: just initialized with these parameters.
        let lane =
            unsafe { ProducerLane::<Payload>::from_block(region.addr(), capacity, consumer_slots) };
        (region, lane)
    }

    fn read(lane: &ProducerLane<Payload>, sequence: usize) -> Payload {
        // SAFETY: `sequence` was published, so its cell is initialized.
        unsafe { lane.payload_ptr(sequence).as_ptr().read() }
    }

    /// Reserves, writes, and publishes one value; `false` on backpressure.
    fn publish_value(lane: &mut ProducerLane<Payload>, value: Payload) -> bool {
        let one = NonZeroUsize::new(1).unwrap();
        let Some(start) = lane.reserve(one) else {
            return false;
        };
        // SAFETY: the cell is reserved and not yet published.
        unsafe { lane.payload_ptr(start).as_ptr().write(value) };
        lane.publish(one);
        true
    }

    #[test]
    fn lane_ownership_is_exclusive() {
        let (_region, lane) = lane(4, 1);
        assert!(lane.try_acquire());
        assert!(!lane.try_acquire());
        lane.release();
        assert!(lane.try_acquire());
    }

    #[test]
    fn publishes_and_advances_cursors() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire());
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value * 10));
        }
        assert_eq!(lane.published(), 4);
        assert_eq!(lane.reserved(), 4);
        for seq in 0..4usize {
            assert_eq!(read(&lane, seq), seq as u64 * 10);
        }
    }

    #[test]
    fn reserves_and_publishes_a_batch() {
        let (_region, mut lane) = lane(8, 1);
        assert!(lane.try_acquire());
        let count = NonZeroUsize::new(3).unwrap();
        let start = lane.reserve(count).expect("reserve");
        for offset in 0..count.get() {
            // SAFETY: each cell in the batch is reserved and unpublished.
            unsafe {
                lane.payload_ptr(start.wrapping_add(offset))
                    .as_ptr()
                    .write((offset as u64) + 1)
            };
        }
        // Reserved but not yet visible.
        assert_eq!(lane.reserved(), 3);
        assert_eq!(lane.published(), 0);
        lane.publish(count);
        assert_eq!(lane.published(), 3);
        for offset in 0..3usize {
            assert_eq!(read(&lane, offset), offset as u64 + 1);
        }
    }

    #[test]
    fn reserve_rejects_count_above_capacity() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire());
        assert!(lane.reserve(NonZeroUsize::new(5).unwrap()).is_none());
    }

    #[test]
    fn no_active_consumers_allows_free_overwrite() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire());
        // Publish well past one revolution; with no active consumer there is
        // nothing to protect, so every reserve succeeds.
        for value in 0..16u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert_eq!(lane.published(), 16);
        // The ring holds the most recent generation (sequences 12..16).
        for seq in 12..16usize {
            assert_eq!(read(&lane, seq), seq as u64);
        }
    }

    #[test]
    fn backpressure_when_consumer_lags() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire());
        // Join consumer 0; nothing published yet, so it starts at sequence 0.
        assert_eq!(lane.join_consumer(0, false), 0);

        // Fill the ring; the consumer has read nothing, so the next reserve laps.
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert!(!publish_value(&mut lane, 99));

        // Consumer consumes sequences 0 and 1; two cells free up.
        lane.set_consumer_cursor(0, 2);
        assert!(publish_value(&mut lane, 100));
        assert!(publish_value(&mut lane, 101));
        // Now full again relative to the watermark (cursor 2, capacity 4).
        assert!(!publish_value(&mut lane, 102));

        // The recycled cells hold the new payloads.
        assert_eq!(read(&lane, 4), 100);
        assert_eq!(read(&lane, 5), 101);
        assert_eq!(lane.published(), 6);
    }

    #[test]
    fn join_starts_at_current_publication() {
        let (_region, mut lane) = lane(8, 1);
        assert!(lane.try_acquire());
        // Publish 3 with no consumer (free overwrite), advancing the publication.
        for value in 0..3u64 {
            assert!(publish_value(&mut lane, value));
        }
        // A consumer joining now starts at the publication, not 0.
        assert_eq!(lane.join_consumer(0, false), 3);
    }

    #[test]
    fn released_consumer_no_longer_constrains() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire());
        assert_eq!(lane.join_consumer(0, false), 0);
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert!(!publish_value(&mut lane, 99));

        // Releasing the slot removes the constraint.
        lane.release_consumer(0);
        assert!(publish_value(&mut lane, 99));
    }
}
