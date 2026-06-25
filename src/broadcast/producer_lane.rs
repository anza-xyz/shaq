//! One producer's lane: a self-contained shared-memory block holding the lane's
//! ownership state, its reserve/publication cursors, the per-consumer reserve
//! limits, and the ring of payloads. See [`ProducerLane`].

use core::mem::{align_of, size_of};
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use core::sync::atomic::{fence, AtomicU64, Ordering};

use crate::CacheAlignedAtomicSize;

use super::consumer_state::LaneConsumerState;

const LANE_FREE: u64 = 0;
const LANE_ACTIVE: u64 = 1;

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
/// The lane holds ownership state, the reserve/publication cursors, the
/// per-lane consumer reserve-limit state, and the ring of `T`. The owning
/// producer mutates the ring and producer cursors (`&mut self`); consumers read
/// published payloads and publish their own progress through [`LaneConsumerState`].
///
/// Block layout: `LaneHeader`, then `[CacheAlignedAtomicSize; consumer_slots]`
/// limits (one per cache line), then `[T; capacity]`.
pub(crate) struct ProducerLane<T> {
    header: NonNull<LaneHeader>,
    consumer_state: LaneConsumerState,
    ring: NonNull<T>,

    mask: usize, // capacity - 1
}

#[inline]
const fn consumer_state_offset() -> usize {
    size_of::<LaneHeader>().next_multiple_of(LaneConsumerState::block_align())
}

#[inline]
fn ring_offset<T>(consumer_slots: usize) -> Option<usize> {
    consumer_state_offset()
        .checked_add(LaneConsumerState::block_size(consumer_slots)?)?
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
        // SAFETY: layout reserves `consumer_slots` aligned slots here.
        let consumer_state = unsafe { block.byte_add(consumer_state_offset()) };
        // SAFETY: freshly initialized lane block; consumer slots are initialized once.
        unsafe { LaneConsumerState::init(consumer_state, consumer_slots) };
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
        //         for consumer state to fit (if init succeeded).
        let consumer_state = unsafe {
            LaneConsumerState::from_block(
                block.byte_add(consumer_state_offset()),
                consumer_slots,
                capacity as usize,
            )
        };
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for ring data to fit (if init succeeded).
        let ring = unsafe {
            block.byte_add(ring_offset::<T>(consumer_slots).expect("validated_lane layout"))
        }
        .cast();

        Self {
            header,
            consumer_state,
            ring,
            mask: (capacity as usize).wrapping_sub(1),
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

    #[inline]
    pub(crate) fn consumer_state(&self) -> LaneConsumerState {
        self.consumer_state
    }

    /// Pointer to the ring cell holding `sequence` — used by the producer to
    /// write a reserved cell and by consumers to read a published one.
    #[inline]
    pub(crate) fn payload_ptr(&self, sequence: usize) -> NonNull<T> {
        // SAFETY: `sequence & mask < capacity`; the ring has `capacity` cells.
        unsafe { self.ring.add(sequence & self.mask) }
    }

    /// Claims the lane for a producer. Returns `false` if already owned.
    #[must_use]
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

    /// Reserves `count` consecutive sequences for writing, returning the first.
    /// `None` on backpressure: the batch would overwrite a cell an active
    /// consumer has not yet read, or it exceeds the ring capacity.
    /// On success this returns Some(seqnum) - with seqnum being
    /// the starting sequence number of the reservation.
    ///
    /// Write each reserved cell via [`Self::payload_ptr`], then [`Self::publish`].
    pub(crate) fn try_reserve(&mut self, count: NonZeroUsize) -> Option<usize> {
        if count.get() > self.capacity() {
            return None;
        }
        let start = self.header().producer_reservation.load(Ordering::Acquire);
        // Producer half of the join handshake: order the previous reserve's
        // `producer_reservation` store before this reserve's limit loads, so a
        // racing consumer join either sees our advance or we see its limit.
        fence(Ordering::SeqCst);
        // Each slot already stores `next_to_read + capacity`, so the gate is a
        // plain comparison: rejecting once the batch would reach a sequence a
        // consumer still needs. Unowned slots sit at the top, so they never
        // gate.
        if start.wrapping_add(count.get()) > self.consumer_state.reserve_limit() {
            return None;
        }
        // Claim before the writes; consumers only read `< producer_publication`.
        self.header()
            .producer_reservation
            .store(start.wrapping_add(count.get()), Ordering::Release);
        Some(start)
    }

    /// Publishes `start..start + count`, making it visible to consumers. Call
    /// after the cells are written.
    pub(crate) fn publish(&mut self, start: usize, count: NonZeroUsize) {
        self.header()
            .producer_publication
            .store(start.wrapping_add(count.get()), Ordering::Release);
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

    fn join_consumer(
        lane: &ProducerLane<Payload>,
        consumer_index: usize,
        from_backlog: bool,
    ) -> usize {
        let consumer_state = lane.consumer_state();
        consumer_state.join(consumer_index, from_backlog, lane.published(), || {
            lane.reserved()
        })
    }

    /// Reserves, writes, and publishes one value; `false` on backpressure.
    fn publish_value(lane: &mut ProducerLane<Payload>, value: Payload) -> bool {
        let one = NonZeroUsize::new(1).unwrap();
        let Some(start) = lane.try_reserve(one) else {
            return false;
        };
        // SAFETY: the cell is reserved and not yet published.
        unsafe { lane.payload_ptr(start).as_ptr().write(value) };
        lane.publish(start, one);
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
        let start = lane.try_reserve(count).expect("reserve");
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
        lane.publish(start, count);
        assert_eq!(lane.published(), 3);
        for offset in 0..3usize {
            assert_eq!(read(&lane, offset), offset as u64 + 1);
        }
    }

    #[test]
    fn reserve_rejects_count_above_capacity() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire());
        assert!(lane.try_reserve(NonZeroUsize::new(5).unwrap()).is_none());
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
        assert_eq!(join_consumer(&lane, 0, false), 0);

        // Fill the ring; the consumer has read nothing, so the next reserve laps.
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert!(!publish_value(&mut lane, 99));

        // Consumer consumes sequences 0 and 1; two cells free up.
        lane.consumer_state().set_cursor(0, 2);
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
    fn released_consumer_no_longer_constrains() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire());
        assert_eq!(join_consumer(&lane, 0, false), 0);
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert!(!publish_value(&mut lane, 99));

        // Releasing the slot removes the constraint.
        lane.consumer_state().release(0);
        assert!(publish_value(&mut lane, 99));
    }
}
