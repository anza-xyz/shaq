//! Multi-producer / multi-consumer broadcast queue in shared memory.
//!
//! Each producer owns its own ring (a `ProducerLane`); every consumer reads
//! every lane, so each published item reaches all consumers. Backpressure is
//! lossless — a producer cannot overwrite a cell until every consumer has read
//! it.
//!
//! [`Producer`] writes via by-value [`Producer::try_write`], an in-place
//! [`WriteGuard`], or a [`WriteBatch`]; [`Consumer`] reads via
//! [`Consumer::try_read`], a [`ReadGuard`], or a [`ReadBatch`], with blocking
//! `*_timeout` variants that park on the queue's futex when idle. A consumer
//! joins at the current frontier, or with the `*_from_backlog` variants up to one
//! ring behind it to also read data published before it joined. After a crash, a
//! lane or consumer index can be taken over with `recover` (resuming where the
//! dead owner was) or returned to the pool with `force_release`.
//!
//! Payloads are treated as shared-memory bytes, not as ordinary Rust-owned
//! values. In practice `T` should be POD-like: no process-local pointers, no
//! references whose provenance matters in another process, no destructor that
//! must run, and safe to duplicate by raw reads. This is stricter than a normal
//! queue because every broadcast consumer can read the same cell.
//!
//! Recovery is an externally serialized operation. Do not race recovery or
//! force-release with other recovery operations, with joins/drops for the same
//! queue, or with a still-live owner of the index/lane being recovered.
//!
//! The region is a fixed header (magic/version, the global consumer-ownership
//! table, and the blocked-consumer wake counter) followed by one lane block per
//! producer.

mod consumer_state;
mod producer_lane;

use core::marker::PhantomData;
use core::mem::size_of;
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use core::sync::atomic::{AtomicU64, Ordering};
use std::fs::File;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, WaitError};
use crate::futex::{Waiters, SPIN_ATTEMPTS};
use crate::shmem::Region;
use crate::{CacheAlignedAtomicSize, VERSION};

use consumer_state::ConsumerState;
use producer_lane::ProducerLane;

const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");

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
/// per-consumer reserve limits live in the lane blocks; consumer-index ownership
/// and the blocked-consumer wake state are global.
#[repr(C)]
struct SharedQueueHeader {
    magic: AtomicU64,
    version: u32,
    capacity: u32, // per-lane ring capacity (power of two)
    producer_slots: u32,
    consumer_slots: u32,
    /// Count of consumers blocked waiting for any lane to publish.
    waiters: Waiters,
    /// Futex word for blocked consumers: bumped only when a publish wakes one
    /// (the lane cursors carry the data; this only breaks a racing wait).
    wake_seq: CacheAlignedAtomicSize,
}

impl SharedQueueHeader {
    /// Initializes the shared queue header and publishes the region.
    ///
    /// # Safety
    /// - `header` must be non-null, properly aligned, and large enough for
    ///   [`SharedQueueHeader`].
    /// - Access to `header` must be unique.
    /// - The queue's non-header sections must already be initialized.
    unsafe fn init(header: NonNull<Self>, layout: &Layout) {
        // SAFETY: caller guarantees valid, uniquely-owned storage for the header.
        unsafe {
            header.as_ptr().write(Self {
                magic: AtomicU64::new(0),
                version: VERSION,
                capacity: layout.capacity,
                producer_slots: layout.producer_slots as u32,
                consumer_slots: layout.consumer_slots as u32,
                waiters: Waiters::default(),
                wake_seq: CacheAlignedAtomicSize::default(),
            });
        }

        // Publish initialization last.
        // SAFETY: the header was initialized by the write above.
        unsafe { header.as_ref() }
            .magic
            .store(MAGIC, Ordering::Release);
    }
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
    fn new<T>(config: &BroadcastConfig) -> Result<Self, Error> {
        Self::checked_new::<T>(config).ok_or(Error::InvalidBufferSize)
    }

    fn checked_new<T>(config: &BroadcastConfig) -> Option<Self> {
        if config.capacity == 0 {
            return None;
        }
        let capacity = config.capacity.checked_next_power_of_two()?;
        if capacity > u32::MAX as usize {
            return None;
        }
        // `consumer_slots == 0` is allowed: producers then run free (no consumer
        // can constrain the reserve limit), useful for measuring a producer in
        // isolation. `producer_slots` must be at least one — a queue with no
        // lanes can hold nothing.
        if config.producer_slots == 0
            || config.producer_slots > u32::MAX as usize
            || config.consumer_slots > u32::MAX as usize
        {
            return None;
        }
        let capacity = capacity as u32;

        let consumer_state_offset =
            size_of::<SharedQueueHeader>().next_multiple_of(ConsumerState::block_align());
        let consumer_state_bytes = ConsumerState::block_size(config.consumer_slots)?;

        let block_align = ProducerLane::<T>::block_align();
        let producer_blocks_offset = consumer_state_offset
            .checked_add(consumer_state_bytes)?
            .checked_next_multiple_of(block_align)?;
        let block_stride = ProducerLane::<T>::block_size(capacity, config.consumer_slots)?
            .checked_next_multiple_of(block_align)?;
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
        // `capacity` is already a power of two, so the recomputed layout must
        // match the stored capacity exactly.
        let layout = Layout::new::<T>(&config)?;
        if layout.capacity as usize != capacity || region_size < layout.total {
            return Err(Error::InvalidBufferSize);
        }
        Ok(layout)
    }
}

/// A handle onto the shared region: the header plus the section base pointers.
struct SharedQueue<T> {
    region: Arc<Region>,
    header: NonNull<SharedQueueHeader>,
    consumer_state: ConsumerState,
    producer_blocks: NonNull<u8>,
    // Runtime scalars (the layout offsets are construction-only, so not kept).
    capacity: u32,
    producer_slots: usize,
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
        let layout = Layout::new::<T>(config)?;
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

        // Global consumer-ownership table: every index free.
        let consumer_state = unsafe { base.byte_add(layout.consumer_state_offset) };
        // SAFETY: the layout reserves `consumer_slots` AtomicU64s here.
        unsafe { ConsumerState::init(consumer_state, layout.consumer_slots) };

        // Producer-lane blocks.
        // SAFETY: the layout reserves `producer_slots` blocks of `block_stride`.
        let producer_blocks = unsafe { base.byte_add(layout.producer_blocks_offset) };
        for lane in 0..layout.producer_slots {
            // SAFETY: `lane < producer_slots`; blocks are `block_stride` apart.
            let block = unsafe { producer_blocks.byte_add(lane.wrapping_mul(layout.block_stride)) };
            // SAFETY: the block is sized for `(capacity, consumer_slots)`.
            unsafe { ProducerLane::<T>::init(block, layout.consumer_slots) };
        }

        // Header initialization publishes the queue, so it runs after every
        // other region section is initialized.
        let header = base.cast::<SharedQueueHeader>();
        // SAFETY: region is page-aligned, large enough for the header, uniquely
        // initialized here, and all non-header sections are initialized above.
        unsafe { SharedQueueHeader::init(header, layout) };
    }

    fn from_region(region: Arc<Region>, layout: Layout) -> Self {
        let base = region.addr();
        let header = base.cast::<SharedQueueHeader>();
        // SAFETY: offsets lie within the region.
        let consumer_state = unsafe {
            ConsumerState::from_block(
                base.byte_add(layout.consumer_state_offset),
                layout.consumer_slots,
            )
        };
        // SAFETY: offsets lie within the region.
        let producer_blocks = unsafe { base.byte_add(layout.producer_blocks_offset) };
        Self {
            region,
            header,
            consumer_state,
            producer_blocks,
            capacity: layout.capacity,
            producer_slots: layout.producer_slots,
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
        unsafe { ProducerLane::from_block(block, self.capacity, self.consumer_slots()) }
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
        self.consumer_state.len()
    }

    #[inline]
    fn header(&self) -> &SharedQueueHeader {
        // SAFETY: the header sits at the region base and outlives this handle.
        unsafe { self.header.as_ref() }
    }

    /// Bumps the wake counter and wakes blocked consumers, if any. Called after
    /// a publish (the lane cursor is already advanced); a no-op on the hot path
    /// when nothing is blocked.
    fn wake(&self) {
        let header = self.header();
        header.waiters.bump_and_wake(&header.wake_seq);
    }

    /// Blocks until `check` succeeds or `timeout` elapses, sleeping on the global
    /// wake counter (a publish on any lane bumps it). `check` reads the lane
    /// cursors, which carry the actual data.
    fn wait_for<R>(
        &self,
        timeout: Duration,
        check: impl FnMut() -> Option<R>,
    ) -> Result<R, WaitError> {
        // `check` scans every lane, so scale the per-check baseline down by the
        // lane count to keep total spin work comparable to a single-cursor queue.
        let spins = (SPIN_ATTEMPTS / self.producer_slots).max(1);
        let header = self.header();
        header
            .waiters
            .wait_for(&header.wake_seq, spins, timeout, check)
    }

    /// Claims a free consumer index in the global ownership table.
    fn acquire_consumer_index(&self) -> Result<usize, Error> {
        self.consumer_state.acquire()
    }

    /// Releases a consumer index back to free.
    fn release_consumer_index(&self, index: usize) {
        self.consumer_state.release(index);
    }

    /// Force-owns a consumer index whose previous owner died (no CAS). The
    /// caller guarantees that owner is dead and no live handle uses the index.
    fn recover_consumer_index(&self, index: usize) {
        self.consumer_state.recover(index);
    }

    /// Maps `file`, initializing it as a broadcast queue.
    ///
    /// # Safety
    /// - `file` must be initialized as a queue at most once (by the designated
    ///   initializer) and not resized while any handle is joined.
    unsafe fn create(file: &File, config: &BroadcastConfig) -> Result<Self, Error> {
        let layout = Layout::new::<T>(config)?;
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
    index: usize,
}

impl<T> Producer<T> {
    /// Creates a broadcast queue in `file` and joins as a producer.
    ///
    /// # Safety
    /// - `file` must be initialized as a broadcast queue exactly once (by the
    ///   designated initializer), and not resized while any handle is joined.
    /// - All producers/consumers for `file` must use the same `T`; queued values
    ///   must be POD-like shared-memory bytes: valid in every process that reads
    ///   them, safe to overwrite without running a destructor, and safe for every
    ///   consumer to duplicate by raw reads.
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
        Ok(Self { queue, lane, index })
    }

    /// The lane this producer owns. Record it so a replacement can
    /// [`recover`](Self::recover) the lane if this producer's process dies.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Takes over a lane whose producer died, without the usual ownership
    /// handshake. The recovered producer continues publishing from the lane's
    /// last published sequence; any cells the dead producer reserved but never
    /// published are discarded.
    ///
    /// # Safety
    /// - All of [`Self::join`]'s requirements, plus: the producer that owned
    ///   `index` must be dead and no other live handle may use that lane — two
    ///   producers on one lane corrupts it.
    /// - Recovery must be serialized externally; it must not race with other
    ///   recovery/force-release operations or with producer/consumer joins or
    ///   drops on the same queue.
    pub unsafe fn recover(file: &File, index: usize) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::join(file) }?;
        Self::recover_in_queue(queue, index)
    }

    fn recover_in_queue(queue: SharedQueue<T>, index: usize) -> Result<Self, Error> {
        if index >= queue.producer_slots() {
            return Err(Error::InvalidIndex);
        }
        let lane = queue.lane(index);
        lane.recover();
        Ok(Self { queue, lane, index })
    }

    /// Force-releases a lane whose producer died, returning it to the free pool
    /// (rewinding its reservation first) so a later [`join`](Self::join) /
    /// [`try_clone`](Self::try_clone) can reclaim it.
    ///
    /// # Safety
    /// - As [`Self::join`], plus: the producer that owned `index` must be dead and
    ///   no other live handle may use that lane.
    /// - Force-release must be serialized externally, with the same restrictions
    ///   as [`Self::recover`].
    pub unsafe fn force_release(file: &File, index: usize) -> Result<(), Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::<T>::join(file) }?;
        if index >= queue.producer_slots() {
            return Err(Error::InvalidIndex);
        }
        queue.lane(index).force_release();
        Ok(())
    }

    /// Claims another free lane on the same queue.
    pub fn try_clone(&self) -> Result<Self, Error> {
        Self::from_queue(self.queue.clone())
    }

    /// Joins the same queue as a consumer (sharing the mapping). The consumer
    /// starts at each lane's current publication, so it only observes items
    /// published after it joins.
    pub fn join_as_consumer(&self) -> Result<Consumer<T>, Error> {
        Consumer::from_queue(self.queue.clone(), false)
    }

    /// Like [`Self::join_as_consumer`], but starts up to one ring behind the
    /// frontier so the consumer reads data published before it joined (see
    /// [`Consumer::join_from_backlog`]).
    pub fn join_as_consumer_from_backlog(&self) -> Result<Consumer<T>, Error> {
        Consumer::from_queue(self.queue.clone(), true)
    }

    /// Publishes one value, or returns it on backpressure (the slowest consumer
    /// has not freed the cell that publishing would overwrite).
    pub fn try_write(&mut self, value: T) -> Result<(), T> {
        // SAFETY: on successful reservation, `value` is written before the guard
        // is dropped and publishes the cell.
        match unsafe { self.try_reserve_write() } {
            Some(guard) => {
                guard.write(value);
                Ok(())
            }
            None => Err(value),
        }
    }

    /// Writes items from a slice into this producer's lane.
    ///
    /// Returns `false` if there is not enough space.
    #[must_use]
    pub fn try_write_slice(&mut self, items: &[T]) -> bool
    where
        T: Copy,
    {
        let Some(len) = NonZeroUsize::new(items.len()) else {
            return true;
        };

        // SAFETY: if reservation succeeds, every reserved cell is written below.
        let mut batch = match unsafe { self.try_reserve_write_batch(len) } {
            Some(batch) => batch,
            None => return false,
        };

        for (index, item) in items.iter().copied().enumerate() {
            // SAFETY: `index` comes from enumerating exactly `len` items.
            unsafe { batch.write(index, item) };
        }
        true
    }

    /// Reserves a single cell for an in-place write, or `None` on backpressure.
    /// The cell becomes visible when the returned guard is dropped.
    ///
    /// # Safety
    /// - The caller must initialize the reserved cell before the guard is
    ///   dropped.
    #[must_use]
    pub unsafe fn try_reserve_write(&mut self) -> Option<WriteGuard<'_, T>> {
        let start = self.lane.try_reserve(NonZeroUsize::MIN)?;
        Some(WriteGuard {
            producer: self,
            start,
        })
    }

    /// Reserves `count` consecutive cells for in-place writes, or `None` on
    /// backpressure. The cells become visible when the returned batch is dropped.
    ///
    /// # Safety
    /// - The caller must initialize every reserved cell before the batch is
    ///   dropped.
    #[must_use]
    pub unsafe fn try_reserve_write_batch(
        &mut self,
        count: NonZeroUsize,
    ) -> Option<WriteBatch<'_, T>> {
        let start = self.lane.try_reserve(count)?;
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
/// [`Self::write`]/[`Self::as_mut_ref`], then drop the guard to publish it.
#[must_use]
pub struct WriteGuard<'a, T> {
    producer: &'a mut Producer<T>,
    start: usize,
}

impl<T> WriteGuard<'_, T> {
    /// Mutable reference to the reserved cell.
    pub fn as_mut_ref(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: forwarded; the cell is reserved for this producer.
        unsafe { &mut *self.producer.lane.payload_ptr(self.start).as_ptr().cast() }
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
        self.producer.lane.publish(self.start, NonZeroUsize::MIN);
        self.producer.queue.wake();
    }
}

/// A reservation of `count` cells in a producer's lane. Write every cell via
/// [`Self::write`]/[`Self::as_mut_ref`], then drop the batch to publish them.
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

    /// Mutable reference to the reserved cell at `index`.
    ///
    /// # Safety
    /// - `index < len`.
    pub unsafe fn as_mut_ref(&mut self, index: usize) -> &mut MaybeUninit<T> {
        debug_assert!(index < self.count.get());
        let ptr = self
            .producer
            .lane
            .payload_ptr(self.start.wrapping_add(index))
            .as_ptr();
        // SAFETY: forwarded; the cell is reserved for this producer.
        unsafe { &mut *ptr.cast() }
    }

    /// Writes `value` into the reserved cell at `index`.
    ///
    /// # Safety
    /// - `index < len`.
    pub unsafe fn write(&mut self, index: usize, value: T) {
        // SAFETY: forwarded; `index < len` and the cell is reserved.
        unsafe { self.as_mut_ref(index).write(value) };
    }
}

impl<T> Drop for WriteBatch<'_, T> {
    fn drop(&mut self) {
        self.producer.lane.publish(self.start, self.count);
        self.producer.queue.wake();
    }
}

/// A consumer: owns one consumer index and reads every lane round-robin. Reads
/// only items published after it joins. Single-threaded use (`&mut self`).
pub struct Consumer<T> {
    queue: SharedQueue<T>,
    index: usize,
    /// One cached view per lane (avoids rebuilding it on every read).
    lanes: Box<[ProducerLane<T>]>,
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
    ///   (the consumer may be the initializer), same POD-like `T` across all
    ///   handles.
    pub unsafe fn create(file: &File, config: BroadcastConfig) -> Result<Self, Error> {
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        let queue = unsafe { SharedQueue::create(file, &config) }?;
        Self::from_queue(queue, false)
    }

    /// Joins an existing broadcast queue in `file` as a consumer, starting at the
    /// current frontier (only items published after the join).
    ///
    /// # Safety
    /// - Same as [`Producer::join`]: live queue, same `T` across all handles.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::join(file) }?;
        Self::from_queue(queue, false)
    }

    /// Like [`Self::join`], but starts up to one ring behind the frontier so the
    /// consumer reads data published before it joined. Best for slow queues; on a
    /// fast one that has already lapped, it falls back to the frontier (see
    /// [`Self::join`]).
    ///
    /// # Safety
    /// - Same as [`Self::join`].
    pub unsafe fn join_from_backlog(file: &File) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::join(file) }?;
        Self::from_queue(queue, true)
    }

    fn from_queue(queue: SharedQueue<T>, from_backlog: bool) -> Result<Self, Error> {
        let index = queue.acquire_consumer_index()?;
        // Cache a view per lane (independent of `queue`), and join each — at the
        // frontier, or up to one ring behind it when `from_backlog`.
        let lanes: Box<[ProducerLane<T>]> = (0..queue.producer_slots())
            .map(|lane| queue.lane(lane))
            .collect();
        let next_by_lane = lanes
            .iter()
            .map(|lane| Self::join_lane(lane, index, from_backlog))
            .collect();
        Ok(Self {
            queue,
            index,
            lanes,
            next_by_lane,
            scan_start_lane: 0,
        })
    }

    /// The consumer index this handle owns. Record it so a replacement can
    /// [`recover`](Self::recover) it if this consumer's process dies.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Takes over a consumer index whose owner died, without the usual ownership
    /// handshake, **resuming where the dead owner left off** on each lane — its
    /// unread items are still pinned by its reserve limit, so they are delivered.
    /// To instead restart fresh, [`force_release`](Self::force_release) the index
    /// and `join` it however you like.
    ///
    /// # Safety
    /// - All of [`Self::join`]'s requirements, plus: the consumer that owned
    ///   `index` must be dead and no other live handle may use it — two consumers
    ///   sharing an index corrupts each other's cursor.
    /// - Recovery must be serialized externally; it must not race with other
    ///   recovery/force-release operations or with producer/consumer joins or
    ///   drops on the same queue.
    pub unsafe fn recover(file: &File, index: usize) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::join(file) }?;
        Self::recover_in_queue(queue, index)
    }

    fn recover_in_queue(queue: SharedQueue<T>, index: usize) -> Result<Self, Error> {
        if index >= queue.consumer_slots() {
            return Err(Error::InvalidIndex);
        }
        queue.recover_consumer_index(index);
        // Resume each lane at the dead owner's recorded position (read-only, so
        // its backpressure is never dropped).
        let lanes: Box<[ProducerLane<T>]> = (0..queue.producer_slots())
            .map(|lane| queue.lane(lane))
            .collect();
        let next_by_lane = lanes
            .iter()
            .map(|lane| Self::recover_lane(lane, index))
            .collect();
        Ok(Self {
            queue,
            index,
            lanes,
            next_by_lane,
            scan_start_lane: 0,
        })
    }

    /// Force-releases a consumer index whose owner died, returning it to the free
    /// pool: the lanes are un-wedged and a later [`join`](Self::join) /
    /// [`join_from_backlog`](Self::join_from_backlog) can reclaim it fresh. Use this
    /// (rather than [`recover`](Self::recover)) when you want to drop the dead
    /// consumer's backlog and choose a new start position.
    ///
    /// # Safety
    /// - As [`Self::join`], plus: the consumer that owned `index` must be dead and
    ///   no other live handle may use it.
    /// - Force-release must be serialized externally, with the same restrictions
    ///   as [`Self::recover`].
    pub unsafe fn force_release(file: &File, index: usize) -> Result<(), Error> {
        // SAFETY: validated against the stored header.
        let queue = unsafe { SharedQueue::<T>::join(file) }?;
        if index >= queue.consumer_slots() {
            return Err(Error::InvalidIndex);
        }
        for lane in 0..queue.producer_slots() {
            queue.lane(lane).consumer_state().release(index);
        }
        queue.release_consumer_index(index);
        Ok(())
    }

    fn join_lane(lane: &ProducerLane<T>, index: usize, from_backlog: bool) -> usize {
        let consumer_state = lane.consumer_state();
        consumer_state.join(index, from_backlog, lane.published(), || lane.reserved())
    }

    fn recover_lane(lane: &ProducerLane<T>, index: usize) -> usize {
        let consumer_state = lane.consumer_state();
        consumer_state.recover(index, lane.published(), || lane.reserved())
    }

    /// Finds the next readable `(lane, sequence)`, scanning round-robin from
    /// `scan_start_lane`. A lane is readable when its publication is ahead of
    /// this consumer's cursor.
    fn next_readable(&self) -> Option<(usize, usize)> {
        let producer_slots = self.queue.producer_slots();
        let mut lane = self.scan_start_lane;
        for _ in 0..producer_slots {
            let next = self.next_by_lane[lane];
            // Cursor wrap is not supported - simple comparison works here.
            if self.lanes[lane].published() > next {
                return Some((lane, next));
            }
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
        self.lanes[lane]
            .consumer_state()
            .set_cursor(self.index, next);
        // `lane < producer_slots`, so the wrap is a conditional subtract.
        let mut scan_start_lane = lane.wrapping_add(1);
        if scan_start_lane == self.queue.producer_slots() {
            scan_start_lane = 0;
        }
        self.scan_start_lane = scan_start_lane;
    }

    /// Reads the next available value by copying it out, or `None` if every lane
    /// is caught up.
    pub fn try_read(&mut self) -> Option<T>
    where
        T: Copy,
    {
        let (lane, sequence) = self.next_readable()?;
        // SAFETY: `sequence < publication`, so the cell is published (initialized);
        // this consumer's cursor still protects it from being overwritten until we
        // advance below. The copy happens before the cursor advances.
        let value = unsafe { self.lanes[lane].payload_ptr(sequence).as_ptr().read() };
        self.advance(lane, NonZeroUsize::MIN);
        Some(value)
    }

    /// Reserves the next available value in place without copying, or `None` if
    /// every lane is caught up. The returned guard holds this consumer's cursor
    /// at the value's sequence, so the producer cannot overwrite it until the
    /// guard is dropped, which advances past it.
    #[must_use]
    pub fn try_reserve_read(&mut self) -> Option<ReadGuard<'_, T>> {
        let (lane, sequence) = self.next_readable()?;
        let payload = self.lanes[lane].payload_ptr(sequence);
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
    /// is dropped, which advances past the whole batch.
    #[must_use]
    pub fn try_reserve_read_batch(&mut self, max: NonZeroUsize) -> Option<ReadBatch<'_, T>> {
        let (lane, start) = self.next_readable()?;
        // `next_readable` guarantees at least one published value past `start`.
        let available = self.lanes[lane].published().wrapping_sub(start);
        let count = available.min(max.get());
        let count = NonZeroUsize::new(count).expect("readable lane has at least one value");
        Some(ReadBatch {
            consumer: self,
            lane,
            start,
            count,
        })
    }

    /// Blocks until any lane has an unread value or `timeout` elapses, then
    /// returns a [`ReadGuard`] for it; `Err(Timeout)` if none arrived in time.
    pub fn reserve_read_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<ReadGuard<'_, T>, WaitError> {
        self.wait_until_readable(timeout)?;
        Ok(self
            .try_reserve_read()
            .expect("a lane is readable after a successful wait"))
    }

    /// Blocks until any lane has unread values or `timeout` elapses, then returns
    /// a [`ReadBatch`] of up to `max` of them; `Err(Timeout)` if none arrived.
    pub fn reserve_read_batch_timeout(
        &mut self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<ReadBatch<'_, T>, WaitError> {
        self.wait_until_readable(timeout)?;
        Ok(self
            .try_reserve_read_batch(max)
            .expect("a lane is readable after a successful wait"))
    }

    /// Blocks until any lane has an unread value or `timeout` elapses, then
    /// copies it out; `Err(Timeout)` if none arrived in time.
    pub fn read_timeout(&mut self, timeout: Duration) -> Result<T, WaitError>
    where
        T: Copy,
    {
        let guard = self.reserve_read_timeout(timeout)?;
        Ok(guard.read())
    }

    /// Blocks until [`Self::next_readable`] would succeed, sleeping on the queue's
    /// global wake counter (a publish on any lane wakes it).
    fn wait_until_readable(&self, timeout: Duration) -> Result<(), WaitError> {
        self.queue
            .wait_for(timeout, || self.next_readable().map(|_| ()))
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Drop each lane's reserve limit first, then the global ownership, so no
        // limit lingers for an index another consumer could reclaim.
        for lane in self.lanes.iter() {
            lane.consumer_state().release(self.index);
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
    /// Copies the value out; the guard advances past it on drop.
    pub fn read(self) -> T
    where
        T: Copy,
    {
        // SAFETY: the cell is published and held by this consumer's cursor.
        unsafe { self.payload.as_ptr().read() }
    }
}

impl<T> AsRef<T> for ReadGuard<'_, T> {
    fn as_ref(&self) -> &T {
        // SAFETY: the cell is published and held by this consumer's cursor.
        unsafe { self.payload.as_ref() }
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
        self.consumer.lanes[self.lane]
            .payload_ptr(self.start.wrapping_add(index))
            .as_ptr()
    }

    /// Reference to the value at `index`.
    ///
    /// # Safety
    /// - `index < len`
    pub unsafe fn as_ref(&self, index: usize) -> &T {
        // SAFETY: the cell is published and held by this consumer's
        // cursor.
        unsafe { &*self.as_ptr(index) }
    }

    /// Copies the value at `index` out.
    ///
    /// # Safety
    /// - `index < len`
    pub unsafe fn read(&self, index: usize) -> T
    where
        T: Copy,
    {
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
                assert_eq!(c.try_read(), Some(value * 10));
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
                // SAFETY: every reserved slot is initialized below.
                let mut batch =
                    unsafe { p.try_reserve_write_batch(NonZeroUsize::new(3).unwrap()) }.unwrap();
                assert_eq!(batch.len(), 3);
                for index in 0..3usize {
                    // SAFETY: index < len.
                    unsafe { batch.write(index, (index as u64) + 1) };
                }
            }
            for value in 1..=3u64 {
                assert_eq!(c.try_read(), Some(value));
            }
        }
    }

    #[test]
    fn try_write_slice_publishes_all_items() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();
            assert!(p.try_write_slice(&[]));
            assert!(p.try_write_slice(&[1, 2, 3]));

            for value in 1..=3u64 {
                assert_eq!(c.try_read(), Some(value));
            }
            assert_eq!(c.try_read(), None);
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

    #[test]
    fn zero_consumer_slots_lets_producer_run_free() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 0,
            });
            // No consumer can ever constrain the lane, so writes never block.
            for value in 0..16u64 {
                assert!(p.try_write(value).is_ok());
            }
            // And no consumer can join when there are no consumer slots.
            assert!(matches!(
                p.join_as_consumer(),
                Err(Error::ConsumerSlotsExhausted)
            ));
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
    fn layout_rejects_oversized_capacity_without_panicking() {
        let config = BroadcastConfig {
            capacity: usize::MAX,
            producer_slots: 1,
            consumer_slots: 1,
        };
        assert!(Layout::new::<Payload>(&config).is_err());
    }

    #[test]
    fn layout_rejects_zero_capacity() {
        let config = BroadcastConfig {
            capacity: 0,
            producer_slots: 1,
            consumer_slots: 1,
        };
        assert!(Layout::new::<Payload>(&config).is_err());
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
                assert_eq!(c0.try_read(), Some(value * 10));
                assert_eq!(c1.try_read(), Some(value * 10));
            }
            assert_eq!(c0.try_read(), None);
            assert_eq!(c1.try_read(), None);

            // A released consumer's index can be reclaimed.
            drop(c1);
            assert!(p.join_as_consumer().is_ok());
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn threaded_producer_publish_is_visible_to_all_consumers() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const ITEMS: usize = 257;
        const BATCH: usize = 7;

        for create in producer_creators() {
            let mut producer = create(BroadcastConfig {
                capacity: 32,
                producer_slots: 1,
                consumer_slots: 2,
            });
            let consumers = [
                producer.join_as_consumer().unwrap(),
                producer.join_as_consumer().unwrap(),
            ];
            let start = Arc::new(Barrier::new(consumers.len() + 1));

            let consumer_threads: Vec<_> = consumers
                .into_iter()
                .map(|mut consumer| {
                    let start = Arc::clone(&start);
                    thread::spawn(move || {
                        start.wait();
                        let mut seen = Vec::with_capacity(ITEMS);
                        for _ in 0..ITEMS {
                            seen.push(
                                consumer
                                    .read_timeout(Duration::from_secs(1))
                                    .expect("published value"),
                            );
                        }
                        assert!(matches!(
                            consumer.read_timeout(Duration::ZERO),
                            Err(WaitError::Timeout)
                        ));
                        seen
                    })
                })
                .collect();

            let producer_start = Arc::clone(&start);
            let producer_thread = thread::spawn(move || {
                producer_start.wait();
                let mut next = 0usize;
                while next < ITEMS {
                    let count = (ITEMS - next).min(BATCH);
                    let mut batch = [0u64; BATCH];
                    for (offset, slot) in batch[..count].iter_mut().enumerate() {
                        *slot = (next + offset) as u64;
                    }
                    if producer.try_write_slice(&batch[..count]) {
                        next += count;
                    } else {
                        thread::yield_now();
                    }
                }
            });

            producer_thread.join().expect("producer thread");
            let expected: Vec<_> = (0..ITEMS as u64).collect();
            for consumer_thread in consumer_threads {
                assert_eq!(consumer_thread.join().expect("consumer thread"), expected);
            }
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
            assert_eq!(c.try_read(), Some(0));
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

            let guard = c.try_reserve_read().expect("readable");
            assert_eq!(*guard.as_ref(), 10);

            // The producer can fill the rest of the ring but cannot overwrite the
            // cell the guard still holds (sequence 0).
            for value in 11..14u64 {
                assert!(p.try_write(value).is_ok());
            }
            assert!(p.try_write(14).is_err());
            assert_eq!(*guard.as_ref(), 10);

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
                // SAFETY: the reserved slot is initialized before `guard` is dropped.
                let mut guard = unsafe { p.try_reserve_write() }.unwrap();
                guard.as_mut_ref().write(42);
            }
            // `write` consumes the guard and publishes on drop too.
            // SAFETY: `write` initializes the reserved slot before publishing.
            unsafe { p.try_reserve_write() }.unwrap().write(43);

            assert_eq!(c.try_reserve_read().unwrap().read(), 42);
            assert_eq!(c.try_reserve_read().unwrap().read(), 43);
            assert!(c.try_reserve_read().is_none());
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
                let batch = c
                    .try_reserve_read_batch(NonZeroUsize::new(3).unwrap())
                    .unwrap();
                assert_eq!(batch.len(), 3);
                for index in 0..3 {
                    // SAFETY: `index < len`; `Payload` is `u64`.
                    assert_eq!(unsafe { batch.read(index) }, index as u64);
                }
            }
            // Now bounded by the available run (2), not `max`.
            {
                let batch = c
                    .try_reserve_read_batch(NonZeroUsize::new(10).unwrap())
                    .unwrap();
                assert_eq!(batch.len(), 2);
                for index in 0..2 {
                    // SAFETY: `index < len`; `Payload` is `u64`.
                    assert_eq!(unsafe { *batch.as_ref(index) }, (index as u64) + 3);
                }
            }
            assert!(c
                .try_reserve_read_batch(NonZeroUsize::new(1).unwrap())
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
                let batch = c
                    .try_reserve_read_batch(NonZeroUsize::new(4).unwrap())
                    .unwrap();
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
            // Joins at the current publication (3), so it sees only later items.
            let mut c = p.join_as_consumer().unwrap();
            assert!(p.try_write(99).is_ok());
            assert_eq!(c.try_read(), Some(99));
            assert_eq!(c.try_read(), None);
        }
    }

    #[test]
    fn consumer_joining_during_unpublished_reservation_waits_for_publication() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);
        let mut producer = Producer::from_queue(queue.clone()).unwrap();

        // SAFETY: the reserved slot is initialized before the guard is dropped.
        let mut guard = unsafe { producer.try_reserve_write() }.unwrap();
        let mut consumer = Consumer::from_queue(queue.clone(), false).unwrap();

        assert!(consumer.try_reserve_read().is_none());
        guard.as_mut_ref().write(42);
        drop(guard);

        assert_eq!(consumer.try_read(), Some(42));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn from_backlog_consumer_reads_old_data_on_a_slow_queue() {
        for create in producer_creators() {
            // The queue has capacity to spare and has not wrapped, so the backlog
            // is fully retained.
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            for value in 0..3u64 {
                assert!(p.try_write(value).is_ok());
            }
            // A from-backlog join starts at the oldest retained item (sequence 0),
            // so it reads data published before it joined, then keeps up.
            let mut c = p.join_as_consumer_from_backlog().unwrap();
            for value in 0..3u64 {
                assert_eq!(c.try_read(), Some(value));
            }
            assert!(p.try_write(99).is_ok());
            assert_eq!(c.try_read(), Some(99));
            assert_eq!(c.try_read(), None);
        }
    }

    #[test]
    fn reserve_read_timeout_times_out_then_observes_publication() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();

            // Nothing published on any lane yet: a zero timeout reports `Timeout`
            // rather than blocking.
            assert!(matches!(
                c.reserve_read_timeout(Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            // A publish on any lane satisfies the (already-elapsed) wait.
            assert!(p.try_write(42).is_ok());
            let guard = c.reserve_read_timeout(Duration::ZERO).expect("readable");
            assert_eq!(*guard.as_ref(), 42);
        }
    }

    /// Exercises the real `FUTEX_WAIT` syscall (not just the elapsed-deadline
    /// short-circuit): with nothing published, a bounded wait blocks in the
    /// kernel on the wake counter and returns `Timeout`.
    #[cfg(not(miri))]
    #[test]
    fn reserve_read_timeout_blocks_in_futex_then_times_out() {
        for create in producer_creators() {
            let p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();
            assert!(matches!(
                c.reserve_read_timeout(Duration::from_millis(5)),
                Err(WaitError::Timeout)
            ));
        }
    }

    #[test]
    fn read_and_batch_timeout_observe_publication() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.join_as_consumer().unwrap();

            assert!(matches!(
                c.read_timeout(Duration::ZERO),
                Err(WaitError::Timeout)
            ));
            assert!(matches!(
                c.reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            for value in 0..2u64 {
                assert!(p.try_write(value).is_ok());
            }
            // The batch sees both published values; dropping it commits them.
            {
                let batch = c
                    .reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO)
                    .expect("readable");
                assert_eq!(batch.len(), 2);
            }
            assert!(matches!(
                c.reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO),
                Err(WaitError::Timeout)
            ));
        }
    }

    /// Allocates a heap-backed queue and returns the shared handle (recovery
    /// tests need the queue directly to simulate a crashed handle).
    fn recovery_queue(config: &BroadcastConfig) -> SharedQueue<Payload> {
        let size = Layout::new::<Payload>(config).expect("layout").total;
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).expect("alloc");
        // SAFETY: freshly allocated region, initialized exactly once.
        unsafe { SharedQueue::<Payload>::create_in_region(&region, config) }.unwrap()
    }

    #[test]
    fn recover_producer_takes_over_a_wedged_lane() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);
        let mut consumer = Consumer::from_queue(queue.clone(), false).unwrap();

        // A producer publishes two items, then its process "crashes". A clean
        // drop would free the lane, so re-mark it ACTIVE to mimic a dead owner
        // that never released it.
        {
            let mut producer = Producer::from_queue(queue.clone()).unwrap();
            assert!(producer.try_write(1).is_ok());
            assert!(producer.try_write(2).is_ok());
        }
        assert!(queue.lane(0).try_acquire());

        // The only lane is held, so a normal join is refused.
        assert!(matches!(
            Producer::from_queue(queue.clone()),
            Err(Error::ProducerSlotsExhausted)
        ));

        // Recover the lane and keep publishing where the dead producer stopped.
        let mut recovered = Producer::recover_in_queue(queue.clone(), 0).unwrap();
        assert_eq!(recovered.index(), 0);
        assert!(recovered.try_write(3).is_ok());

        // The consumer (joined before the crash) sees every item in order.
        for value in 1..=3u64 {
            assert_eq!(consumer.try_read(), Some(value));
        }
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn recover_producer_rewinds_unpublished_reservation() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);

        // Mimic a producer that reserved a batch but crashed before publishing:
        // the reservation runs ahead of the publication.
        let mut lane = queue.lane(0);
        assert!(lane.try_acquire());
        lane.try_reserve(NonZeroUsize::new(3).unwrap());
        assert_eq!(lane.reserved(), 3);
        assert_eq!(lane.published(), 0);

        // Recovery rewinds the reservation back to the publication.
        let recovered = Producer::recover_in_queue(queue.clone(), 0).unwrap();
        assert_eq!(recovered.lane.reserved(), 0);
        assert_eq!(recovered.lane.published(), 0);
    }

    #[test]
    fn recover_consumer_resumes_from_last_position() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);

        // A consumer joins at sequence 0, then its process "crashes" with a read
        // position recorded (next-to-read = 1). Build that state directly (claim
        // the index, join, record the cursor) so nothing releases the slot.
        let index = queue.acquire_consumer_index().unwrap();
        let lane = queue.lane(0);
        Consumer::join_lane(&lane, index, false);

        let mut producer = Producer::from_queue(queue.clone()).unwrap();
        for value in 0..3u64 {
            assert!(producer.try_write(value).is_ok());
        }
        lane.consumer_state().set_cursor(index, 1);

        // Recovery resumes at the recorded position: it reads the unread backlog
        // (items 1, 2), never re-reading item 0.
        let mut recovered = Consumer::recover_in_queue(queue.clone(), index).unwrap();
        assert_eq!(recovered.index(), index);
        assert_eq!(recovered.try_read(), Some(1));
        assert_eq!(recovered.try_read(), Some(2));
        assert_eq!(recovered.try_read(), None);
    }

    #[test]
    fn force_release_consumer_frees_the_index_for_a_fresh_join() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);

        // The only consumer index is claimed and the lane's limit recorded, then
        // its owner "crashes" (no release).
        let index = queue.acquire_consumer_index().unwrap();
        let lane = queue.lane(0);
        Consumer::join_lane(&lane, index, false);
        let mut producer = Producer::from_queue(queue.clone()).unwrap();
        for value in 0..3u64 {
            assert!(producer.try_write(value).is_ok());
        }
        lane.consumer_state().set_cursor(index, 1);

        // A fresh join can't proceed — the index is still owned.
        assert!(matches!(
            Consumer::from_queue(queue.clone(), false),
            Err(Error::ConsumerSlotsExhausted)
        ));

        // Force-release frees it (clear limits + free the index); a catch-up join
        // then reclaims it and reads the still-present backlog from the start.
        for lane in 0..queue.producer_slots() {
            queue.lane(lane).consumer_state().release(index);
        }
        queue.release_consumer_index(index);
        let mut fresh = Consumer::from_queue(queue.clone(), true).unwrap();
        assert_eq!(fresh.index(), index);
        for value in 0..3u64 {
            assert_eq!(fresh.try_read(), Some(value));
        }
        assert_eq!(fresh.try_read(), None);
    }

    #[test]
    fn recover_rejects_out_of_range_index() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);
        assert!(matches!(
            Producer::recover_in_queue(queue.clone(), 1),
            Err(Error::InvalidIndex)
        ));
        assert!(matches!(
            Consumer::recover_in_queue(queue.clone(), 1),
            Err(Error::InvalidIndex)
        ));
    }
}
