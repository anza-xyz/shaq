//! Multi-producer / multi-consumer broadcast queue.
//!
//! Each producer owns a [`producer_lane::ProducerLane`]; every consumer reads
//! every lane. This module wires the shared region (header + global consumer
//! ownership + producer-lane blocks) and the [`Producer`] handle.

// Some of this (e.g. the global consumer-ownership table, used by the consumer
// side) has no non-test callers, so dead-code warnings are silenced.
#![allow(dead_code)]

mod producer_lane;

use core::marker::PhantomData;
use core::mem::{align_of, size_of};
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use core::sync::atomic::{AtomicU64, Ordering};
use std::fs::File;
use std::sync::Arc;

use crate::error::Error;
use crate::shmem::Region;
use crate::{normalized_capacity, VERSION};

use producer_lane::ProducerLane;

const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");

/// Consumer-index ownership states for the global `consumer_state` table, read
/// and written by the consumer side.
const CONSUMER_FREE: u64 = 0;
const CONSUMER_ACTIVE: u64 = 1;

/// Runtime configuration for a broadcast queue.
///
/// `capacity` is the per-lane ring capacity (rounded up to a power of two);
/// `producer_slots` / `consumer_slots` bound the lanes / consumers.
pub struct BroadcastConfig {
    pub capacity: usize,
    pub producer_slots: usize,
    pub consumer_slots: usize,
}

/// Shared header at the start of the region. The producer-lane cursors and the
/// per-consumer reserve limits live in the lane blocks; only consumer-index
/// ownership is global (the `consumer_state` table after this header).
#[repr(C)]
struct SharedQueueHeader {
    magic: AtomicU64,
    version: u32,
    capacity: u32, // per-lane ring capacity (power of two)
    producer_slots: u32,
    consumer_slots: u32,
}

/// Byte offsets and sizes of the region's sections (a construction-time helper;
/// `SharedQueue` keeps only the runtime scalars from it).
struct Layout {
    capacity: u32,
    producer_slots: usize,
    consumer_slots: usize,
    consumer_state_offset: usize,
    producer_blocks_offset: usize,
    block_stride: usize,
    total: usize,
}

impl Layout {
    fn new<T>(config: &BroadcastConfig) -> Option<Self> {
        let capacity = normalized_capacity(config.capacity);
        if capacity == 0 || capacity > u32::MAX as usize {
            return None;
        }
        if config.producer_slots == 0
            || config.producer_slots > u32::MAX as usize
            || config.consumer_slots == 0
            || config.consumer_slots > u32::MAX as usize
        {
            return None;
        }
        let capacity = capacity as u32;

        let consumer_state_offset =
            size_of::<SharedQueueHeader>().next_multiple_of(align_of::<AtomicU64>());
        let consumer_state_bytes = config.consumer_slots.checked_mul(size_of::<AtomicU64>())?;

        let block_align = ProducerLane::<T>::block_align();
        let producer_blocks_offset = consumer_state_offset
            .checked_add(consumer_state_bytes)?
            .checked_next_multiple_of(block_align)?;
        let block_stride = ProducerLane::<T>::block_size(capacity, config.consumer_slots)
            .next_multiple_of(block_align);
        let producer_blocks_bytes = block_stride.checked_mul(config.producer_slots)?;
        let total = producer_blocks_offset.checked_add(producer_blocks_bytes)?;

        Some(Self {
            capacity,
            producer_slots: config.producer_slots,
            consumer_slots: config.consumer_slots,
            consumer_state_offset,
            producer_blocks_offset,
            block_stride,
            total,
        })
    }

    /// Reconstructs and validates the layout from an initialized header.
    fn from_header<T>(header: &SharedQueueHeader, region_size: usize) -> Result<Self, Error> {
        let capacity = header.capacity as usize;
        let config = BroadcastConfig {
            capacity,
            producer_slots: header.producer_slots as usize,
            consumer_slots: header.consumer_slots as usize,
        };
        // `capacity` is already a power of two; `normalized_capacity` is a no-op,
        // so the recomputed layout must match the stored capacity exactly.
        let layout = Layout::new::<T>(&config).ok_or(Error::InvalidBufferSize)?;
        if layout.capacity as usize != capacity || region_size < layout.total {
            return Err(Error::InvalidBufferSize);
        }
        Ok(layout)
    }
}

/// A handle onto the shared region: the header plus the section base pointers.
/// Cloneable — every producer/consumer shares one mapping.
struct SharedQueue<T> {
    region: Arc<Region>,
    header: NonNull<SharedQueueHeader>,
    consumer_state: NonNull<AtomicU64>,
    producer_blocks: NonNull<u8>,
    // Runtime scalars (the layout offsets are construction-only, so not kept).
    capacity: u32,
    producer_slots: usize,
    consumer_slots: usize,
    block_stride: usize,
    _marker: PhantomData<T>,
}

impl<T> SharedQueue<T> {
    /// Initializes a broadcast region and returns a handle.
    ///
    /// # Safety
    /// - `region` must be initialized as a broadcast queue at most once.
    unsafe fn create_in_region(
        region: &Arc<Region>,
        config: &BroadcastConfig,
    ) -> Result<Self, Error> {
        let layout = Layout::new::<T>(config).ok_or(Error::InvalidBufferSize)?;
        if region.size() < layout.total {
            return Err(Error::InvalidBufferSize);
        }
        // SAFETY: region is large enough and (per the contract) initialized once.
        unsafe { Self::initialize(region, &layout) };
        Ok(Self::from_region(Arc::clone(region), layout))
    }

    /// Validates an initialized broadcast region and returns a handle.
    ///
    /// # Safety
    /// - `region` must reference memory laid out by [`Self::create_in_region`].
    unsafe fn join_region(region: &Arc<Region>) -> Result<Self, Error> {
        let header = region.addr().cast::<SharedQueueHeader>();
        // SAFETY: regions are page-aligned (>= align_of::<SharedQueueHeader>()).
        let header_ref = unsafe { header.as_ref() };
        if header_ref.magic.load(Ordering::Acquire) != MAGIC {
            return Err(Error::InvalidMagic);
        }
        if header_ref.version != VERSION {
            return Err(Error::InvalidVersion {
                expected: VERSION,
                actual: header_ref.version,
            });
        }
        let layout = Layout::from_header::<T>(header_ref, region.size())?;
        Ok(Self::from_region(Arc::clone(region), layout))
    }

    /// # Safety
    /// - `region` must be at least `layout.total` bytes and initialized once.
    unsafe fn initialize(region: &Arc<Region>, layout: &Layout) {
        let base = region.addr();
        // SAFETY: regions are page-aligned and large enough for the header.
        let header = unsafe { &mut *base.cast::<SharedQueueHeader>().as_ptr() };
        header.version = VERSION;
        header.capacity = layout.capacity;
        header.producer_slots = layout.producer_slots as u32;
        header.consumer_slots = layout.consumer_slots as u32;

        // Global consumer-ownership table: every index free.
        // SAFETY: the layout reserves `consumer_slots` AtomicU64s here.
        let consumer_state = unsafe {
            base.byte_add(layout.consumer_state_offset)
                .cast::<AtomicU64>()
        };
        for index in 0..layout.consumer_slots {
            // SAFETY: `index < consumer_slots`.
            unsafe {
                consumer_state
                    .add(index)
                    .as_ptr()
                    .write(AtomicU64::new(CONSUMER_FREE))
            };
        }

        // Producer-lane blocks.
        // SAFETY: the layout reserves `producer_slots` blocks of `block_stride`.
        let producer_blocks = unsafe { base.byte_add(layout.producer_blocks_offset) };
        for lane in 0..layout.producer_slots {
            // SAFETY: `lane < producer_slots`; blocks are `block_stride` apart.
            let block = unsafe { producer_blocks.byte_add(lane.wrapping_mul(layout.block_stride)) };
            // SAFETY: the block is sized for `(capacity, consumer_slots)`.
            unsafe { ProducerLane::<T>::init(block, layout.consumer_slots) };
        }

        // Publish initialization last.
        header.magic.store(MAGIC, Ordering::Release);
    }

    fn from_region(region: Arc<Region>, layout: Layout) -> Self {
        let base = region.addr();
        let header = base.cast::<SharedQueueHeader>();
        // SAFETY: offsets lie within the region.
        let consumer_state = unsafe { base.byte_add(layout.consumer_state_offset).cast() };
        // SAFETY: offsets lie within the region.
        let producer_blocks = unsafe { base.byte_add(layout.producer_blocks_offset) };
        Self {
            region,
            header,
            consumer_state,
            producer_blocks,
            capacity: layout.capacity,
            producer_slots: layout.producer_slots,
            consumer_slots: layout.consumer_slots,
            block_stride: layout.block_stride,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn producer_slots(&self) -> usize {
        self.producer_slots
    }

    /// A view over producer lane `lane`.
    fn lane(&self, lane: usize) -> ProducerLane<T> {
        debug_assert!(lane < self.producer_slots);
        // SAFETY: `lane < producer_slots`; blocks are `block_stride` apart.
        let block = unsafe {
            self.producer_blocks
                .byte_add(lane.wrapping_mul(self.block_stride))
        };
        // SAFETY: the block was initialized with these parameters.
        unsafe { ProducerLane::from_block(block, self.capacity, self.consumer_slots) }
    }

    /// Claims a free producer lane, returning its index.
    fn acquire_producer_lane(&self) -> Result<usize, Error> {
        for lane in 0..self.producer_slots {
            if self.lane(lane).try_acquire() {
                return Ok(lane);
            }
        }
        Err(Error::ProducerSlotsExhausted)
    }

    #[inline]
    fn consumer_slots(&self) -> usize {
        self.consumer_slots
    }

    /// The global ownership word for consumer index `index`.
    #[inline]
    fn consumer_state(&self, index: usize) -> &AtomicU64 {
        debug_assert!(index < self.consumer_slots);
        // SAFETY: `index < consumer_slots`; the slot was initialized.
        unsafe { &*self.consumer_state.add(index).as_ptr() }
    }

    /// Claims a free consumer index in the global ownership table.
    fn acquire_consumer_index(&self) -> Result<usize, Error> {
        for index in 0..self.consumer_slots {
            if self
                .consumer_state(index)
                .compare_exchange(
                    CONSUMER_FREE,
                    CONSUMER_ACTIVE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(index);
            }
        }
        Err(Error::ConsumerSlotsExhausted)
    }

    /// Releases a consumer index back to free.
    fn release_consumer_index(&self, index: usize) {
        self.consumer_state(index)
            .store(CONSUMER_FREE, Ordering::Release);
    }

    /// Maps `file`, initializing it as a broadcast queue.
    ///
    /// # Safety
    /// - `file` must be initialized as a queue at most once (by the designated
    ///   initializer) and not resized while any handle is joined.
    unsafe fn create(file: &File, config: &BroadcastConfig) -> Result<Self, Error> {
        let layout = Layout::new::<T>(config).ok_or(Error::InvalidBufferSize)?;
        file.set_len(layout.total as u64)?;
        let region = Region::map_file(file, layout.total)?;
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        unsafe { Self::create_in_region(&region, config) }
    }

    /// Maps and validates an existing broadcast queue in `file`.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue, not resized while joined.
    unsafe fn join(file: &File) -> Result<Self, Error> {
        let file_size = file.metadata()?.len() as usize;
        let region = Region::map_file(file, file_size)?;
        // SAFETY: validated against the stored header.
        unsafe { Self::join_region(&region) }
    }
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            region: Arc::clone(&self.region),
            header: self.header,
            consumer_state: self.consumer_state,
            producer_blocks: self.producer_blocks,
            capacity: self.capacity,
            producer_slots: self.producer_slots,
            consumer_slots: self.consumer_slots,
            block_stride: self.block_stride,
            _marker: PhantomData,
        }
    }
}

// SAFETY: the region is shared (file-backed / heap) and access is synchronized
// by the queue protocol; the pointers are stable for the region's lifetime.
unsafe impl<T: Send> Send for SharedQueue<T> {}
unsafe impl<T: Send> Sync for SharedQueue<T> {}

/// A producer: owns one lane and publishes into it. Single-threaded use (its
/// write ops take `&mut self`); move it between threads to hand off ownership.
///
/// Holds the lane view directly (stable for the producer's lifetime); the
/// `queue` is kept for `try_clone` and to keep the region mapping alive.
pub struct Producer<T> {
    queue: SharedQueue<T>,
    lane: ProducerLane<T>,
}

impl<T> Producer<T> {
    /// Creates a broadcast queue in `file` and joins as a producer.
    ///
    /// # Safety
    /// - `file` must be initialized as a broadcast queue exactly once (by the
    ///   designated initializer), and not resized while any handle is joined.
    /// - All producers/consumers for `file` must use the same `T`; queued values
    ///   must be valid to read/overwrite as shared-memory bytes in each process.
    pub unsafe fn create(file: &File, config: BroadcastConfig) -> Result<Self, Error> {
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        let queue = unsafe { SharedQueue::create(file, &config) }?;
        Self::from_queue(queue)
    }

    /// Joins an existing broadcast queue in `file` as a producer.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue (not resized while joined),
    ///   with the same `T` as every other handle (see [`Self::create`]).
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::join(file) }?;
        Self::from_queue(queue)
    }

    fn from_queue(queue: SharedQueue<T>) -> Result<Self, Error> {
        let index = queue.acquire_producer_lane()?;
        let lane = queue.lane(index);
        Ok(Self { queue, lane })
    }

    /// Claims another free lane on the same queue.
    pub fn try_clone(&self) -> Result<Self, Error> {
        Self::from_queue(self.queue.clone())
    }

    /// Joins the same queue as a consumer (sharing the mapping). The consumer
    /// starts at each lane's current reservation, so it only observes items
    /// published after it joins.
    pub fn join_as_consumer(&self) -> Result<Consumer<T>, Error> {
        Consumer::from_queue(self.queue.clone())
    }

    /// Publishes one value, or returns it on backpressure (the slowest consumer
    /// has not freed the cell that publishing would overwrite).
    pub fn try_write(&mut self, value: T) -> Result<(), T> {
        match self.reserve_write() {
            Some(guard) => {
                guard.write(value);
                Ok(())
            }
            None => Err(value),
        }
    }

    /// Reserves a single cell for an in-place write, or `None` on backpressure.
    /// The cell becomes visible when the returned guard is dropped; the caller
    /// must write it first.
    #[must_use]
    pub fn reserve_write(&mut self) -> Option<WriteGuard<'_, T>> {
        let start = self.lane.reserve(NonZeroUsize::MIN)?;
        Some(WriteGuard {
            producer: self,
            start,
        })
    }

    /// Reserves `count` consecutive cells for in-place writes, or `None` on
    /// backpressure. The cells become visible when the returned batch is dropped;
    /// the caller must write all of them first.
    #[must_use]
    pub fn reserve_write_batch(&mut self, count: NonZeroUsize) -> Option<WriteBatch<'_, T>> {
        let start = self.lane.reserve(count)?;
        Some(WriteBatch {
            producer: self,
            start,
            count,
        })
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.lane.release();
    }
}

// SAFETY: a producer is single-threaded (write ops take `&mut self`) but may be
// moved between threads.
unsafe impl<T: Send> Send for Producer<T> {}

/// A reservation of one cell in a producer's lane. Write it via
/// [`Self::write`]/[`Self::as_mut_ptr`], then drop the guard to publish it.
#[must_use]
pub struct WriteGuard<'a, T> {
    producer: &'a mut Producer<T>,
    start: usize,
}

impl<T> WriteGuard<'_, T> {
    /// Pointer to the reserved cell.
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.producer.lane.payload_ptr(self.start).as_ptr()
    }

    /// Mutable reference to the reserved cell.
    ///
    /// # Safety
    /// - The cell is uninitialized; `T` must be valid for any bytes (the caller
    ///   must not read it before fully initializing it).
    pub unsafe fn as_mut_ref(&mut self) -> &mut T {
        // SAFETY: forwarded; the cell is reserved for this producer.
        unsafe { &mut *self.as_mut_ptr() }
    }

    /// Writes `value` into the reserved cell; the guard publishes it on drop.
    pub fn write(self, value: T) {
        // SAFETY: the cell is reserved and not yet published; `T` is moved in.
        unsafe {
            self.producer
                .lane
                .payload_ptr(self.start)
                .as_ptr()
                .write(value)
        };
    }
}

impl<T> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        self.producer.lane.publish(NonZeroUsize::MIN);
    }
}

/// A reservation of `count` cells in a producer's lane. Write every cell via
/// [`Self::write`]/[`Self::as_mut_ptr`], then drop the batch to publish them.
#[must_use]
pub struct WriteBatch<'a, T> {
    producer: &'a mut Producer<T>,
    start: usize,
    count: NonZeroUsize,
}

impl<T> WriteBatch<'_, T> {
    pub fn len(&self) -> usize {
        self.count.get()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// Pointer to the reserved cell at `index`.
    ///
    /// # Safety
    /// - `index < len`; the cell is uninitialized and the caller must fully
    ///   initialize `T` before the batch is dropped.
    pub unsafe fn as_mut_ptr(&mut self, index: usize) -> *mut T {
        debug_assert!(index < self.count.get());
        self.producer
            .lane
            .payload_ptr(self.start.wrapping_add(index))
            .as_ptr()
    }

    /// Writes `value` into the reserved cell at `index`.
    ///
    /// # Safety
    /// - `index < len`.
    pub unsafe fn write(&mut self, index: usize, value: T) {
        // SAFETY: forwarded; `index < len` and the cell is reserved.
        unsafe { self.as_mut_ptr(index).write(value) };
    }
}

impl<T> Drop for WriteBatch<'_, T> {
    fn drop(&mut self) {
        self.producer.lane.publish(self.count);
    }
}

/// A consumer: owns one consumer index and reads every lane round-robin. Reads
/// only items published after it joins. Single-threaded use (`&mut self`).
pub struct Consumer<T> {
    queue: SharedQueue<T>,
    index: usize,
    /// Next sequence to read per lane (local cache of the published cursors).
    next_by_lane: Box<[usize]>,
    /// Lane to start the next round-robin scan from (rotates for fairness).
    scan_start_lane: usize,
}

impl<T> Consumer<T> {
    /// Creates a broadcast queue in `file` and joins as a consumer.
    ///
    /// # Safety
    /// - Same as [`Producer::create`]: `file` initialized as a queue exactly once
    ///   (the consumer may be the initializer), same `T` across all handles.
    pub unsafe fn create(file: &File, config: BroadcastConfig) -> Result<Self, Error> {
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        let queue = unsafe { SharedQueue::create(file, &config) }?;
        Self::from_queue(queue)
    }

    /// Joins an existing broadcast queue in `file` as a consumer.
    ///
    /// # Safety
    /// - Same as [`Producer::join`]: live queue, same `T` across all handles.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::join(file) }?;
        Self::from_queue(queue)
    }

    fn from_queue(queue: SharedQueue<T>) -> Result<Self, Error> {
        let index = queue.acquire_consumer_index()?;
        let producer_slots = queue.producer_slots();
        // Join each lane at its current reservation; cache the start positions.
        let mut next_by_lane = Vec::with_capacity(producer_slots);
        for lane in 0..producer_slots {
            next_by_lane.push(queue.lane(lane).join_consumer(index));
        }
        Ok(Self {
            queue,
            index,
            next_by_lane: next_by_lane.into_boxed_slice(),
            scan_start_lane: 0,
        })
    }

    /// Finds the next readable `(lane, sequence)`, scanning round-robin from
    /// `scan_start_lane`. A lane is readable when its publication is ahead of
    /// this consumer's cursor.
    fn next_readable(&self) -> Option<(usize, usize)> {
        let producer_slots = self.queue.producer_slots();
        let mut lane = self.scan_start_lane;
        for _ in 0..producer_slots {
            let next = self.next_by_lane[lane];
            if self.queue.lane(lane).published().wrapping_sub(next) > 0 {
                return Some((lane, next));
            }
            // `lane < producer_slots`, so the wrap is a conditional subtract, not
            // a division.
            lane = lane.wrapping_add(1);
            if lane == producer_slots {
                lane = 0;
            }
        }
        None
    }

    /// Advances this consumer's cursor on `lane` by `count` consumed values,
    /// publishing the progress and rotating the scan start. The consumer is the
    /// sole reader of its own cursor (`&mut self`), so the cursor only moves here
    /// and advancing is a plain increment.
    fn advance(&mut self, lane: usize, count: NonZeroUsize) {
        let next = self.next_by_lane[lane].wrapping_add(count.get());
        self.next_by_lane[lane] = next;
        self.queue.lane(lane).set_consumer_cursor(self.index, next);
        // `lane < producer_slots`, so the wrap is a conditional subtract.
        let mut scan_start_lane = lane.wrapping_add(1);
        if scan_start_lane == self.queue.producer_slots() {
            scan_start_lane = 0;
        }
        self.scan_start_lane = scan_start_lane;
    }

    /// Reads the next available value by copying it out, or `None` if every lane
    /// is caught up.
    ///
    /// # Safety
    /// - `T` must be valid to read from shared-memory bytes in this process.
    pub unsafe fn try_read(&mut self) -> Option<T> {
        let (lane, sequence) = self.next_readable()?;
        // SAFETY: `sequence < publication`, so the cell is published (initialized);
        // this consumer's cursor still protects it from being overwritten until we
        // advance below. The copy happens before the cursor advances.
        let value = unsafe { self.queue.lane(lane).payload_ptr(sequence).as_ptr().read() };
        self.advance(lane, NonZeroUsize::MIN);
        Some(value)
    }

    /// Reserves the next available value in place without copying, or `None` if
    /// every lane is caught up. The returned guard holds this consumer's cursor
    /// at the value's sequence, so the producer cannot overwrite it until the
    /// guard is committed (dropping without committing leaves the cursor for a
    /// re-read).
    #[must_use]
    pub fn reserve_read(&mut self) -> Option<ReadGuard<'_, T>> {
        let (lane, sequence) = self.next_readable()?;
        let payload = self.queue.lane(lane).payload_ptr(sequence);
        Some(ReadGuard {
            consumer: self,
            lane,
            payload,
        })
    }

    /// Reserves up to `max` consecutive published values from the next readable
    /// lane, or `None` if every lane is caught up. A batch reads from a single
    /// lane, so its length is bounded by that lane's available run as well as by
    /// `max`. The returned guard holds this consumer's cursor at the batch start,
    /// so the producer cannot overwrite any of the batched cells until the guard
    /// is committed (or dropped, which leaves the cursor for a re-read).
    #[must_use]
    pub fn reserve_read_batch(&mut self, max: NonZeroUsize) -> Option<ReadBatch<'_, T>> {
        let (lane, start) = self.next_readable()?;
        // `next_readable` guarantees at least one published value past `start`.
        let available = self.queue.lane(lane).published().wrapping_sub(start);
        let count = available.min(max.get());
        let count = NonZeroUsize::new(count).expect("readable lane has at least one value");
        Some(ReadBatch {
            consumer: self,
            lane,
            start,
            count,
        })
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Drop each lane's reserve limit first, then the global ownership, so no
        // limit lingers for an index another consumer could reclaim.
        for lane in 0..self.queue.producer_slots() {
            self.queue.lane(lane).release_consumer(self.index);
        }
        self.queue.release_consumer_index(self.index);
    }
}

// SAFETY: a consumer is single-threaded (read ops take `&mut self`) but may be
// moved between threads.
unsafe impl<T: Send> Send for Consumer<T> {}

/// An in-place borrow of one published value. The consumer's cursor stays at
/// this value's sequence while the guard lives (so the producer cannot overwrite
/// it); dropping the guard advances past it.
#[must_use]
pub struct ReadGuard<'a, T> {
    consumer: &'a mut Consumer<T>,
    lane: usize,
    payload: NonNull<T>,
}

impl<T> ReadGuard<'_, T> {
    /// Pointer to the borrowed value.
    pub fn as_ptr(&self) -> *const T {
        self.payload.as_ptr()
    }

    /// Reference to the borrowed value.
    ///
    /// # Safety
    /// - `T` must be valid to reference as shared-memory bytes in this process.
    pub unsafe fn as_ref(&self) -> &T {
        // SAFETY: the cell is published and held by this consumer's cursor.
        unsafe { self.payload.as_ref() }
    }

    /// Copies the value out; the guard advances past it on drop.
    ///
    /// # Safety
    /// - `T` must be valid to read from shared-memory bytes in this process.
    pub unsafe fn read(self) -> T {
        // SAFETY: the cell is published and held by this consumer's cursor.
        unsafe { self.payload.as_ptr().read() }
    }
}

impl<T> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        self.consumer.advance(self.lane, NonZeroUsize::MIN);
    }
}

/// An in-place borrow of `count` consecutive published values from one lane. The
/// consumer's cursor stays at the batch start while the guard lives (so the
/// producer cannot overwrite any of them); dropping the guard advances past the
/// whole batch.
#[must_use]
pub struct ReadBatch<'a, T> {
    consumer: &'a mut Consumer<T>,
    lane: usize,
    start: usize,
    count: NonZeroUsize,
}

impl<T> ReadBatch<'_, T> {
    pub fn len(&self) -> usize {
        self.count.get()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// Pointer to the value at `index`.
    ///
    /// # Safety
    /// - `index < len`.
    pub unsafe fn as_ptr(&self, index: usize) -> *const T {
        debug_assert!(index < self.count.get());
        self.consumer
            .queue
            .lane(self.lane)
            .payload_ptr(self.start.wrapping_add(index))
            .as_ptr()
    }

    /// Reference to the value at `index`.
    ///
    /// # Safety
    /// - `index < len`; `T` must be valid to reference as shared-memory bytes in
    ///   this process, and the reference must not be used after the guard is
    ///   dropped.
    pub unsafe fn as_ref(&self, index: usize) -> &T {
        // SAFETY: forwarded; the cell is published and held by this consumer's
        // cursor.
        unsafe { &*self.as_ptr(index) }
    }

    /// Copies the value at `index` out.
    ///
    /// # Safety
    /// - `index < len`; `T` must be valid to read from shared-memory bytes in
    ///   this process.
    pub unsafe fn read(&self, index: usize) -> T {
        // SAFETY: forwarded; the cell is published and held by this consumer's
        // cursor until the batch is dropped.
        unsafe { self.as_ptr(index).read() }
    }
}

impl<T> Drop for ReadBatch<'_, T> {
    fn drop(&mut self) {
        self.consumer.advance(self.lane, self.count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(miri))]
    use crate::shmem::create_temp_shmem_file;

    type Payload = u64;
    type CreateProducer = fn(BroadcastConfig) -> Producer<Payload>;

    /// In-process (heap-backed) producer. The `Producer` keeps the region alive
    /// via its `SharedQueue`'s `Arc<Region>`.
    fn create_heap_producer(config: BroadcastConfig) -> Producer<Payload> {
        let size = Layout::new::<Payload>(&config).expect("layout").total;
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).expect("alloc");
        // SAFETY: freshly allocated region, initialized exactly once here.
        let queue = unsafe { SharedQueue::<Payload>::create_in_region(&region, &config) }.unwrap();
        Producer::from_queue(queue).unwrap()
    }

    /// File-backed producer (mmap). Not run under miri (no mmap).
    #[cfg(not(miri))]
    fn create_file_backed_producer(config: BroadcastConfig) -> Producer<Payload> {
        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: a fresh temp file, initialized exactly once here.
        unsafe { Producer::create(&file, config) }.expect("create")
    }

    /// Every behavioral test runs against both backings (heap always; file-backed
    /// when not under miri), matching the other queues.
    fn producer_creators() -> &'static [CreateProducer] {
        &[
            create_heap_producer,
            #[cfg(not(miri))]
            create_file_backed_producer,
        ]
    }

    #[test]
    fn clones_until_lanes_exhausted() {
        for create in producer_creators() {
            let p0 = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            // Two lanes total: the original plus one clone exhausts them.
            let p1 = p0.try_clone().unwrap();
            assert!(matches!(p0.try_clone(), Err(Error::ProducerSlotsExhausted)));
            drop(p1);
            // The freed lane can be reclaimed.
            assert!(p0.try_clone().is_ok());
        }
    }

    #[test]
    fn try_write_publishes() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();
            for value in 0..5u64 {
                assert!(p.try_write(value * 10).is_ok());
            }
            for value in 0..5u64 {
                // SAFETY: `Payload` is `u64`, valid for any bytes.
                assert_eq!(unsafe { c.try_read() }, Some(value * 10));
            }
        }
    }

    #[test]
    fn write_batch_publishes_on_drop() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();
            {
                let mut batch = p
                    .reserve_write_batch(NonZeroUsize::new(3).unwrap())
                    .unwrap();
                assert_eq!(batch.len(), 3);
                for index in 0..3usize {
                    // SAFETY: index < len.
                    unsafe { batch.write(index, (index as u64) + 1) };
                }
            }
            for value in 1..=3u64 {
                // SAFETY: `Payload` is `u64`, valid for any bytes.
                assert_eq!(unsafe { c.try_read() }, Some(value));
            }
        }
    }

    #[test]
    fn backpressure_without_consumers_is_only_capacity() {
        // No consumers: the producer overwrites freely past one revolution.
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            for value in 0..16u64 {
                assert!(p.try_write(value).is_ok());
            }
        }
    }

    /// Only a file can be opened before it holds a valid queue (e.g. by another
    /// process); a heap region is always initialized in-process before it is
    /// joined, so there is no uninitialized heap case to reject.
    #[cfg(not(miri))]
    #[test]
    fn join_rejects_uninitialized_file() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let size = Layout::new::<Payload>(&config).unwrap().total;
        let file = create_temp_shmem_file().expect("temp file");
        file.set_len(size as u64).expect("set_len");
        // SAFETY: sized-but-zeroed file → magic mismatch.
        let err = unsafe { SharedQueue::<Payload>::join(&file) };
        assert!(matches!(err, Err(Error::InvalidMagic)));
    }

    #[test]
    fn every_consumer_observes_every_item_in_order() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 2,
            });
            // Both consumers join before anything is published, so they start at 0.
            let mut c0 = p.join_as_consumer().unwrap();
            let mut c1 = p.join_as_consumer().unwrap();
            // A third consumer would exhaust the slots.
            assert!(matches!(
                p.join_as_consumer(),
                Err(Error::ConsumerSlotsExhausted)
            ));

            for value in 0..5u64 {
                assert!(p.try_write(value * 10).is_ok());
            }
            for value in 0..5u64 {
                // SAFETY: `Payload` is `u64`, valid for any bytes.
                assert_eq!(unsafe { c0.try_read() }, Some(value * 10));
                assert_eq!(unsafe { c1.try_read() }, Some(value * 10));
            }
            // SAFETY: see above.
            assert_eq!(unsafe { c0.try_read() }, None);
            assert_eq!(unsafe { c1.try_read() }, None);

            // A released consumer's index can be reclaimed.
            drop(c1);
            assert!(p.join_as_consumer().is_ok());
        }
    }

    #[test]
    fn slow_consumer_backpressures_producer() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();

            // Fill the ring; the consumer has read nothing, so the producer blocks.
            for value in 0..4u64 {
                assert!(p.try_write(value).is_ok());
            }
            assert!(p.try_write(99).is_err());

            // The consumer reads one; one cell frees up.
            // SAFETY: `Payload` is `u64`.
            assert_eq!(unsafe { c.try_read() }, Some(0));
            assert!(p.try_write(99).is_ok());
            assert!(p.try_write(100).is_err());
        }
    }

    #[test]
    fn read_guard_holds_cell_until_dropped() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();
            assert!(p.try_write(10).is_ok());

            let guard = c.reserve_read().expect("readable");
            // SAFETY: the cell is published.
            assert_eq!(unsafe { *guard.as_ref() }, 10);

            // The producer can fill the rest of the ring but cannot overwrite the
            // cell the guard still holds (sequence 0).
            for value in 11..14u64 {
                assert!(p.try_write(value).is_ok());
            }
            assert!(p.try_write(14).is_err());
            // SAFETY: the guard still protects sequence 0.
            assert_eq!(unsafe { *guard.as_ref() }, 10);

            // Dropping the guard commits, advancing past the cell and freeing it.
            drop(guard);
            assert!(p.try_write(14).is_ok());
        }
    }

    #[test]
    fn write_guard_publishes_on_drop_and_read_guard_reads() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();

            // A single write guard publishes its cell when dropped.
            {
                let mut guard = p.reserve_write().unwrap();
                // SAFETY: `Payload` is `u64`, valid for any bytes.
                unsafe { *guard.as_mut_ref() = 42 };
            }
            // `write` consumes the guard and publishes on drop too.
            p.reserve_write().unwrap().write(43);

            // SAFETY: `Payload` is `u64`; `read` copies out, committing on drop.
            assert_eq!(unsafe { c.reserve_read().unwrap().read() }, 42);
            assert_eq!(unsafe { c.reserve_read().unwrap().read() }, 43);
            assert!(c.reserve_read().is_none());
        }
    }

    #[test]
    fn read_batch_is_bounded_and_commits_on_drop() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();
            for value in 0..5u64 {
                assert!(p.try_write(value).is_ok());
            }

            // Bounded by `max` (3) below the available run (5); drop commits it.
            {
                let batch = c.reserve_read_batch(NonZeroUsize::new(3).unwrap()).unwrap();
                assert_eq!(batch.len(), 3);
                for index in 0..3 {
                    // SAFETY: `index < len`; `Payload` is `u64`.
                    assert_eq!(unsafe { batch.read(index) }, index as u64);
                }
            }
            // Now bounded by the available run (2), not `max`.
            {
                let batch = c
                    .reserve_read_batch(NonZeroUsize::new(10).unwrap())
                    .unwrap();
                assert_eq!(batch.len(), 2);
                for index in 0..2 {
                    // SAFETY: `index < len`; `Payload` is `u64`.
                    assert_eq!(unsafe { *batch.as_ref(index) }, (index as u64) + 3);
                }
            }
            assert!(c
                .reserve_read_batch(NonZeroUsize::new(1).unwrap())
                .is_none());
        }
    }

    #[test]
    fn read_batch_holds_cells_until_dropped() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();
            for value in 0..4u64 {
                assert!(p.try_write(value).is_ok());
            }

            // The batch holds the whole ring; the producer is fully blocked.
            {
                let batch = c.reserve_read_batch(NonZeroUsize::new(4).unwrap()).unwrap();
                assert_eq!(batch.len(), 4);
                assert!(p.try_write(99).is_err());
            }
            // Dropping the batch commits all four cells, freeing them.
            for value in 99..103u64 {
                assert!(p.try_write(value).is_ok());
            }
        }
    }

    #[test]
    fn consumer_joining_late_skips_earlier_items() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            for value in 0..3u64 {
                assert!(p.try_write(value).is_ok());
            }
            // Joins at the current reservation (3), so it sees only later items.
            let mut c = p.join_as_consumer().unwrap();
            assert!(p.try_write(99).is_ok());
            // SAFETY: `Payload` is `u64`.
            assert_eq!(unsafe { c.try_read() }, Some(99));
            assert_eq!(unsafe { c.try_read() }, None);
        }
    }
}
