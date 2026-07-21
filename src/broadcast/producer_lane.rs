//! One producer's lane: a self-contained shared-memory block holding the lane's
//! ownership state, its reserve/publication cursors, the per-consumer reserve
//! limits, and the ring of payloads. See [`ProducerLane`].

use core::alloc::Layout;
use core::mem::{align_of, size_of};
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use core::sync::atomic::{fence, AtomicU64, Ordering};

use crate::CacheAlignedAtomicSize;

use super::consumer_state::LaneConsumerState;

const LANE_FREE: u64 = 0;
const LANE_ACTIVE: u64 = 1;
const LANE_RETIRED: u64 = 2;

/// Fixed-size head of a producer-lane block.
#[repr(C)]
struct LaneHeader {
    /// Lane ownership: `LANE_FREE`, `LANE_ACTIVE`, or `LANE_RETIRED`.
    state: AtomicU64,
    metadata: LaneMetadata,
    /// Claimed-up-to sequence: advanced before a ring cell is written.
    producer_reservation: CacheAlignedAtomicSize,
    /// Visible-up-to sequence: advanced after a ring cell is written; consumers
    /// read sequences `< producer_publication`.
    producer_publication: CacheAlignedAtomicSize,
}

/// A single producer's lane.
///
/// The lane holds ownership state, the reserve/publication cursors, the
/// per-lane consumer reserve-limit state, and the ring of fixed-size payload
/// cells. The owning
/// producer mutates the ring and producer cursors (`&mut self`); consumers read
/// published payloads and publish their own progress through [`LaneConsumerState`].
///
/// Block layout: `LaneHeader`, then `[CacheAlignedAtomicSize; consumer_slots]`
/// limits (one per cache line), then the payload ring.
pub(crate) struct ProducerLane {
    header: NonNull<LaneHeader>,
    consumer_state: LaneConsumerState,
    ring: NonNull<u8>,
    payload_size: usize,

    mask: usize, // capacity - 1
}

#[repr(C)]
struct LaneMetadata {
    /// Queue-assigned identifier of the lane's owning producer.
    /// `0` if the lane has never been owned.
    identifier: AtomicU64,
    /// Count of messages refused by backpressure
    rejected_items: AtomicU64,
}

impl Default for LaneMetadata {
    fn default() -> Self {
        Self {
            identifier: AtomicU64::new(0),
            rejected_items: AtomicU64::new(0),
        }
    }
}

impl LaneMetadata {
    /// Installs the owner's identifier.
    fn install_owner(&self, identifier: u64) {
        self.identifier.store(identifier, Ordering::Release);
    }

    /// Advisory snapshot of [`LaneMetadata`]
    fn snapshot(&self) -> LaneMetadataSnapshot {
        LaneMetadataSnapshot {
            identifier: self.identifier.load(Ordering::Acquire),
            rejected_items: self.rejected_items.load(Ordering::Relaxed),
        }
    }
}

struct LaneMetadataSnapshot {
    identifier: u64,
    rejected_items: u64,
}

#[inline]
const fn consumer_state_offset() -> usize {
    size_of::<LaneHeader>().next_multiple_of(LaneConsumerState::block_align())
}

pub(crate) fn ring_offset_for_payload(
    consumer_slots: usize,
    payload_layout: Layout,
) -> Option<usize> {
    if payload_layout.align() > ProducerLane::block_align() {
        return None;
    }

    consumer_state_offset()
        .checked_add(LaneConsumerState::block_size(consumer_slots)?)?
        .checked_next_multiple_of(payload_layout.align())
}

pub(crate) fn block_size_for_payload(
    capacity: u32,
    consumer_slots: usize,
    payload_layout: Layout,
) -> Option<usize> {
    ring_offset_for_payload(consumer_slots, payload_layout)?
        .checked_add((capacity as usize).checked_mul(payload_layout.size())?)
}

impl ProducerLane {
    pub(crate) const fn block_align() -> usize {
        align_of::<LaneHeader>()
    }

    /// Initializes a lane block: ownership/cursors zeroed, every consumer slot
    /// free. The ring is left uninitialized (each cell is written before it is
    /// published, and read only after).
    ///
    /// # Safety
    /// - `block` must point at a [`block_size_for_payload`] region for the
    ///   queue's `(capacity, consumer_slots, payload_layout)` and be initialized
    ///   at most once.
    pub(crate) unsafe fn init(block: NonNull<u8>, consumer_slots: usize) {
        let header = LaneHeader {
            state: AtomicU64::new(LANE_FREE),
            metadata: LaneMetadata::default(),
            producer_reservation: CacheAlignedAtomicSize::default(),
            producer_publication: CacheAlignedAtomicSize::default(),
        };
        // SAFETY: `block` begins with a `LaneHeader`.
        unsafe { block.cast().write(header) };
        // SAFETY: layout reserves `consumer_slots` aligned slots here.
        let consumer_state = unsafe { block.byte_add(consumer_state_offset()) };
        // SAFETY: freshly initialized lane block; consumer slots are initialized once.
        unsafe { LaneConsumerState::init(consumer_state, consumer_slots) };
    }

    /// Builds a lane view over an initialized block.
    ///
    /// # Safety
    /// - `block` must reference a block initialized by [`Self::init`] with the
    ///   same `consumer_slots`, sized for `(capacity, payload_layout)`, alive
    ///   for the view's use.
    pub(crate) unsafe fn from_block(
        block: NonNull<u8>,
        capacity: u32,
        consumer_slots: usize,
        payload_layout: Layout,
    ) -> Self {
        debug_assert!(capacity.is_power_of_two());

        let header = block.cast();
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for consumer state to fit (if init succeeded).
        let consumer_state_block = unsafe { block.byte_add(consumer_state_offset()) };
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for consumer state to fit (if init succeeded).
        let consumer_state = unsafe {
            LaneConsumerState::from_block(consumer_state_block, consumer_slots, capacity as usize)
        };
        let ring_offset =
            ring_offset_for_payload(consumer_slots, payload_layout).expect("validated_lane layout");
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for ring data to fit (if init succeeded).
        let ring = unsafe { block.byte_add(ring_offset) };

        Self {
            header,
            consumer_state,
            ring,
            payload_size: payload_layout.size(),
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
    pub(crate) fn payload_ptr(&self, sequence: usize) -> NonNull<u8> {
        let offset = (sequence & self.mask).wrapping_mul(self.payload_size);
        // SAFETY: `sequence & mask < capacity`; the ring has `capacity` cells of
        // `payload_size` bytes. Zero-sized payloads always point at the ring base.
        unsafe { self.ring.byte_add(offset) }
    }

    /// Claims the lane for a producer, installing its queue-assigned
    /// `identifier`. Returns `false` if already owned.
    #[must_use]
    pub(crate) fn try_acquire(&self, identifier: u64) -> bool {
        let header = self.header();
        if header
            .state
            .compare_exchange(LANE_FREE, LANE_ACTIVE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        header.metadata.install_owner(identifier);
        true
    }

    /// Permanently retires the lane. A retired lane never returns to the free
    /// pool, so a lane binds to at most one producer for the queue's lifetime.
    pub(crate) fn retire(&self) {
        let _ = self.header().state.compare_exchange(
            LANE_ACTIVE,
            LANE_RETIRED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
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
        // `producer_reservation` store before this reserve's limit loads. A
        // racing consumer publishes a limit from its first reservation sample,
        // fences, then samples again. Either this reserve observes that limit or
        // the consumer observes our committed frontier. If both race with this
        // reserve's later store, the consumer starts at `start`, making this
        // reservation future data rather than an overwrite.
        fence(Ordering::SeqCst);
        // Each slot already stores `next_to_read + capacity`, so the gate is a
        // plain comparison: rejecting once the batch would reach a sequence a
        // consumer still needs. Unowned slots sit at the top, so they never
        // gate.
        if start.wrapping_add(count.get()) > self.consumer_state.reserve_limit() {
            self.header()
                .metadata
                .rejected_items
                .fetch_add(count.get() as u64, Ordering::Relaxed);
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
    pub(crate) fn identifier(&self) -> u64 {
        self.header().metadata.identifier.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        self.header().state.load(Ordering::Acquire) == LANE_ACTIVE
    }

    pub(crate) fn metadata(&self) -> super::LaneMetadata {
        let metadata_snapshot = self.header().metadata.snapshot();
        let is_active = self.is_active();

        super::LaneMetadata {
            identifier: metadata_snapshot.identifier,
            rejected_items: metadata_snapshot.rejected_items,
            is_active,
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
    use assert_matches::assert_matches;
    use std::num::NonZeroUsize;

    type Payload = u64;

    /// Allocates and initializes a standalone lane block.
    fn lane(capacity: u32, consumer_slots: usize) -> (std::sync::Arc<Region>, ProducerLane) {
        let payload_layout = Layout::new::<Payload>();
        let size = block_size_for_payload(capacity, consumer_slots, payload_layout).unwrap();
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
        // SAFETY: freshly allocated block, initialized once.
        unsafe { ProducerLane::init(region.addr(), consumer_slots) };
        // SAFETY: just initialized with these parameters.
        let lane = unsafe {
            ProducerLane::from_block(region.addr(), capacity, consumer_slots, payload_layout)
        };
        (region, lane)
    }

    fn read(lane: &ProducerLane, sequence: usize) -> Payload {
        // SAFETY: `sequence` was published, so its cell is initialized.
        unsafe { lane.payload_ptr(sequence).cast().read() }
    }

    fn join_consumer(lane: &ProducerLane, consumer_index: usize) -> usize {
        let consumer_state = lane.consumer_state();
        consumer_state.join(consumer_index, || lane.reserved())
    }

    /// Reserves, writes, and publishes one value; `false` on backpressure.
    fn publish_value(lane: &mut ProducerLane, value: Payload) -> bool {
        let one = NonZeroUsize::new(1).unwrap();
        let Some(start) = lane.try_reserve(one) else {
            return false;
        };
        // SAFETY: the cell is reserved and not yet published.
        unsafe { lane.payload_ptr(start).cast().write(value) };
        lane.publish(start, one);
        true
    }

    #[test]
    fn lane_ownership_is_exclusive() {
        let (_region, lane) = lane(4, 1);
        assert!(lane.try_acquire(1));
        assert!(!lane.try_acquire(2));
    }

    #[test]
    fn retired_lane_is_never_reclaimed() {
        let (_region, lane) = lane(4, 1);
        assert!(lane.try_acquire(1));
        lane.retire();
        assert!(!lane.try_acquire(2));
    }

    /// Acquires the lane for identifier 42 and fills its ring against a
    /// gating consumer, then gets one single-item reserve refused so the lane
    /// records one rejected item.
    fn owned_lane_with_one_rejected_item(lane: &mut ProducerLane) {
        assert!(lane.try_acquire(42));
        assert_eq!(join_consumer(lane, 0), 0);
        for value in 0..4u64 {
            assert!(publish_value(lane, value));
        }
        assert!(!publish_value(lane, 99));
    }

    #[test]
    fn never_owned_lane_reports_sentinel_metadata() {
        // Given a freshly initialized lane.
        let (_region, lane) = lane(4, 1);

        // Then it reports no owner and the `0` identifier sentinel.
        assert!(!lane.is_active());
        assert_eq!(lane.identifier(), 0);
    }

    #[test]
    fn acquire_installs_the_owner_metadata() {
        // Given a free lane.
        let (_region, lane) = lane(4, 1);

        // When a producer acquires it with identifier 42.
        assert!(lane.try_acquire(42));

        // Then the lane is active under that identifier with a clean
        // rejected-items counter.
        assert!(lane.is_active());
        assert_eq!(lane.identifier(), 42);
        assert_eq!(lane.metadata().rejected_items, 0);
    }

    #[test]
    fn refused_reserves_count_rejected_items() {
        // Given an owned lane whose ring is full against a gating consumer.
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(42));
        assert_eq!(join_consumer(&lane, 0), 0);
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }

        // When a single-item and a two-item reserve are refused.
        assert!(!publish_value(&mut lane, 99));
        assert_matches!(lane.try_reserve(NonZeroUsize::new(2).unwrap()), None);

        // Then every refused item is counted.
        assert_eq!(lane.metadata().rejected_items, 3);
    }

    #[test]
    fn retired_lane_keeps_the_last_owner_metadata() {
        // Given an owned lane holding one rejected item.
        let (_region, mut lane) = lane(4, 1);
        owned_lane_with_one_rejected_item(&mut lane);

        // When the owner drops, retiring the lane.
        lane.retire();

        // Then the last owner's metadata stays readable (post-mortem
        // visibility).
        assert!(!lane.is_active());
        assert_eq!(lane.identifier(), 42);
        assert_eq!(lane.metadata().rejected_items, 1);
    }

    #[test]
    fn publishes_and_advances_cursors() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(1));
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
        assert!(lane.try_acquire(1));
        let count = NonZeroUsize::new(3).unwrap();
        let start = lane.try_reserve(count).expect("reserve");
        for offset in 0..count.get() {
            // SAFETY: each cell in the batch is reserved and unpublished.
            unsafe {
                lane.payload_ptr(start.wrapping_add(offset))
                    .cast()
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
        assert!(lane.try_acquire(1));
        assert!(lane.try_reserve(NonZeroUsize::new(5).unwrap()).is_none());
        // A caller error is not backpressure, so it is not counted as
        // rejected items.
        assert_eq!(lane.metadata().rejected_items, 0);
    }

    #[test]
    fn no_active_consumers_allows_free_overwrite() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(1));
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
        assert!(lane.try_acquire(1));
        // Join consumer 0; nothing published yet, so it starts at sequence 0.
        assert_eq!(join_consumer(&lane, 0), 0);

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
        assert!(lane.try_acquire(1));
        assert_eq!(join_consumer(&lane, 0), 0);
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert!(!publish_value(&mut lane, 99));

        // Releasing the slot removes the constraint.
        lane.consumer_state().release(0);
        assert!(publish_value(&mut lane, 99));
    }
}
