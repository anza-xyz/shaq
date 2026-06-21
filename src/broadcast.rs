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
        let layout = Layout::new::<T>(&config).ok_or(Error::InvalidBufferSize)?;
        file.set_len(layout.total as u64)?;
        let region = Region::map_file(file, layout.total)?;
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        let queue = unsafe { SharedQueue::create_in_region(&region, &config) }?;
        Self::from_queue(queue)
    }

    /// Joins an existing broadcast queue in `file` as a producer.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue (not resized while joined),
    ///   with the same `T` as every other handle (see [`Self::create`]).
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let file_size = file.metadata()?.len() as usize;
        let region = Region::map_file(file, file_size)?;
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::join_region(&region) }?;
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

    /// Publishes one value, or returns it on backpressure (the slowest consumer
    /// has not freed the cell that publishing would overwrite).
    pub fn try_write(&mut self, value: T) -> Result<(), T> {
        let Some(start) = self.lane.reserve(NonZeroUsize::MIN) else {
            return Err(value);
        };
        // SAFETY: the cell is reserved and not yet published; `T` is moved in.
        unsafe { self.lane.payload_ptr(start).as_ptr().write(value) };
        self.lane.publish(start, NonZeroUsize::MIN);
        Ok(())
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
        self.producer.lane.publish(self.start, self.count);
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

    /// Reads a published payload from `lane` at `sequence` (test-only).
    /// Reads a published payload from the producer's own lane (test-only).
    fn read(producer: &Producer<Payload>, sequence: usize) -> Payload {
        // SAFETY: `sequence` was published on this lane, so the cell is initialized.
        unsafe { producer.lane.payload_ptr(sequence).as_ptr().read() }
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
            for value in 0..5u64 {
                assert!(p.try_write(value * 10).is_ok());
            }
            for sequence in 0..5usize {
                assert_eq!(read(&p, sequence), sequence as u64 * 10);
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
            for sequence in 0..3usize {
                assert_eq!(read(&p, sequence), sequence as u64 + 1);
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

    /// File-backed create then join a second producer (exercises `Producer::join`).
    #[cfg(not(miri))]
    #[test]
    fn file_backed_join_acquires_a_second_lane() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 2,
            consumer_slots: 1,
        };
        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: fresh file, initialized once by `create`.
        let _p0 = unsafe { Producer::<Payload>::create(&file, config) }.unwrap();
        // SAFETY: same live queue, same `T`.
        let _p1 = unsafe { Producer::<Payload>::join(&file) }.unwrap();
        // Both lanes are held now; a third join is refused.
        // SAFETY: same live queue, same `T`.
        let third = unsafe { Producer::<Payload>::join(&file) };
        assert!(matches!(third, Err(Error::ProducerSlotsExhausted)));
    }

    #[test]
    fn join_validates_magic_and_version() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let size = Layout::new::<Payload>(&config).unwrap().total;
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
        // SAFETY: fresh region, initialized once.
        let _queue = unsafe { SharedQueue::<Payload>::create_in_region(&region, &config) }.unwrap();
        // SAFETY: initialized above.
        assert!(unsafe { SharedQueue::<Payload>::join_region(&region) }.is_ok());

        let uninit = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
        // SAFETY: uninitialized region → magic mismatch.
        let err = unsafe { SharedQueue::<Payload>::join_region(&uninit) };
        assert!(matches!(err, Err(Error::InvalidMagic)));
    }
}
