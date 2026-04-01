//! DPDK-style bounded MPMC ring queue

use crate::{
    error::Error, normalized_capacity, shmem::MappedRegion, CacheAlignedAtomicSize, VERSION,
};
use core::{marker::PhantomData, ptr::NonNull, sync::atomic::Ordering};
use std::{
    fs::File,
    sync::{atomic::AtomicU64, Arc},
};

/// Unique identifier for MPMC queue in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqmpmc");

pub struct Producer<T> {
    queue: SharedQueue<T>,
}

impl<T> Producer<T> {
    /// Creates a new producer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The provided file must be uniquely created as a Producer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::create::<T>(file, file_size)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Consumer`]s that are joined with
    ///   the same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    pub fn join_as_consumer(&self) -> Consumer<T> {
        Consumer {
            queue: self.queue.clone(),
        }
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation backing `region` must be of sufficient size.
    unsafe fn from_header(
        region: Arc<MappedRegion>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - allocation at `header` is large enough to hold the header and the buffer.
            queue: unsafe { SharedQueue::from_header(region, header) }?,
        })
    }

    /// Writes item into the queue or returns it if there is not enough space.
    pub fn try_write(&self, item: T) -> Result<(), T> {
        // SAFETY: On successful reservation the item is written below.
        let guard = match unsafe { self.reserve_write() } {
            Some(guard) => guard,
            None => return Err(item),
        };
        guard.write(item);
        Ok(())
    }

    /// Writes items from a slice into the queue.
    ///
    /// Returns `Err()` if there is not enough space.
    pub fn try_write_slice(&self, items: &[T]) -> bool
    where
        T: Copy,
    {
        if items.is_empty() {
            return true;
        }

        // SAFETY: if successful we write all items below.
        let mut guard = match unsafe { self.reserve_write_batch(items.len()) } {
            Some(guard) => guard,
            None => return false,
        };

        for (index, item) in items.iter().copied().enumerate() {
            // SAFETY: index is not out of bounds.
            unsafe { guard.write(index, item) };
        }
        true
    }

    /// Reserves a slot for writing.
    /// The slot is committed when the guard is dropped.
    ///
    /// Other [`Producer`]s may write in parallel, but writes must be
    /// published in order they were reserved. Holding a [`WriteGuard`] should
    /// be treated similarly to holding a lock on a critical section.
    ///
    /// # Safety
    /// - The caller must initialize the reserved slot before the guard is dropped.
    #[must_use]
    pub unsafe fn reserve_write(&self) -> Option<WriteGuard<'_, T>> {
        self.queue
            .reserve_write()
            .map(|(cell, position)| WriteGuard {
                header: self.queue.header,
                cell,
                start: position,
                _marker: PhantomData,
            })
    }

    /// Reserves exactly `count` slots for writing.
    /// The slots are committed when the batch is dropped.
    ///
    /// Other [`Producer`]s may write in parallel, but writes must be
    /// published in the order they were reserved. Holding a [`WriteBatch`]
    /// should be treated similarly to holding a lock on a critical section.
    ///
    /// # Safety
    /// - The caller must initialize all reserved slots before the batch is dropped.
    #[must_use]
    pub unsafe fn reserve_write_batch(&self, count: usize) -> Option<WriteBatch<'_, T>> {
        let start = self.queue.reserve_write_batch(count)?;
        Some(WriteBatch {
            header: self.queue.header,
            buffer: self.queue.buffer,
            start,
            count,
            buffer_mask: self.queue.buffer_mask,
            _marker: PhantomData,
        })
    }
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

unsafe impl<T> Send for Producer<T> {}
unsafe impl<T> Sync for Producer<T> {}

pub struct Consumer<T> {
    queue: SharedQueue<T>,
}

impl<T> Consumer<T> {
    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The provided file must be uniquely created as a Consumer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::create::<T>(file, file_size)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing consumer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Producer`]s that are joined with
    ///   the same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> Producer<T> {
        Producer {
            queue: self.queue.clone(),
        }
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation backing `region` must be of sufficient size.
    unsafe fn from_header(
        region: Arc<MappedRegion>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - allocation at `header` is large enough to hold the header and the buffer.
            queue: unsafe { SharedQueue::from_header(region, header) }?,
        })
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    pub fn try_read(&self) -> Option<T> {
        self.reserve_read().map(ReadGuard::read)
    }

    /// Attempts to reserve a value from the queue, returning a guard.
    /// The slot is released back to producers when the guard is dropped.
    ///
    /// Other [`Consumer`]s may read in parallel, but reads must be
    /// released in order they were reserved. Holding a [`ReadGuard`] should
    /// be treated similarly to holding a lock on a critical section.
    #[must_use]
    pub fn reserve_read(&self) -> Option<ReadGuard<'_, T>> {
        self.queue.reserve_read().map(|(cell, position)| ReadGuard {
            header: self.queue.header,
            cell,
            start: position,
            _marker: PhantomData,
        })
    }

    /// Attempts to reserve up to `max` values from the queue.
    /// The slots are released back to producers when the batch is dropped.
    ///
    ///
    /// Other [`Consumer`]s may read in parallel, but reads must be
    /// released in order they were reserved. Holding a [`ReadBatch`] should
    /// be treated similarly to holding a lock on a critical section.
    #[must_use]
    pub fn reserve_read_batch(&self, max: usize) -> Option<ReadBatch<'_, T>> {
        let (start, count) = self.queue.reserve_read_batch(max)?;
        Some(ReadBatch {
            header: self.queue.header,
            buffer: self.queue.buffer,
            start,
            count,
            buffer_mask: self.queue.buffer_mask,
            _marker: PhantomData,
        })
    }
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

unsafe impl<T> Send for Consumer<T> {}
unsafe impl<T> Sync for Consumer<T> {}

/// Calculates the minimum file size required for a queue with given capacity.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
pub const fn minimum_file_size<T: Sized>(capacity: usize) -> usize {
    let buffer_offset = SharedQueueHeader::buffer_offset::<T>();
    buffer_offset + normalized_capacity(capacity) * core::mem::size_of::<T>()
}

struct SharedQueue<T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    buffer_mask: usize,

    // NB: Region must be declared last so it is dropped last ensuring `header` and
    // `buffer` remain valid for their entire lifetime.
    region: Arc<MappedRegion>,
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            header: self.header,
            buffer: self.buffer,
            buffer_mask: self.buffer_mask,
            region: Arc::clone(&self.region),
        }
    }
}

impl<T> SharedQueue<T> {
    #[inline]
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    fn reserve_write(&self) -> Option<(NonNull<T>, usize)> {
        let position = self.reserve_write_batch(1)?;
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, position))
    }

    fn reserve_read(&self) -> Option<(NonNull<T>, usize)> {
        let (position, _) = self.reserve_read_batch(1)?;
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, position))
    }

    fn reserve_write_batch(&self, count: usize) -> Option<usize> {
        if count == 0 {
            return None;
        }

        let capacity = self.capacity();
        if count > capacity {
            return None;
        }

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut producer_reservation = header.producer_reservation.load(Ordering::Relaxed);

        loop {
            let consumer_release = header.consumer_release.load(Ordering::Acquire);
            let used = producer_reservation.wrapping_sub(consumer_release);
            let limit = capacity - count;
            if used > limit {
                return None;
            }
            let new_reservation = producer_reservation.wrapping_add(count);
            match header.producer_reservation.compare_exchange_weak(
                producer_reservation,
                new_reservation,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(producer_reservation);
                }
                Err(current) => {
                    producer_reservation = current;
                }
            }
        }
    }

    fn reserve_read_batch(&self, max: usize) -> Option<(usize, usize)> {
        if max == 0 {
            return None;
        }

        let capacity = self.capacity();
        let max = max.min(capacity);

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut consumer_reservation = header.consumer_reservation.load(Ordering::Relaxed);

        loop {
            let producer_publication = header.producer_publication.load(Ordering::Acquire);
            let available = producer_publication.wrapping_sub(consumer_reservation);
            if available == 0 || available > capacity {
                return None;
            }

            let count = available.min(max);
            let new_reservation = consumer_reservation.wrapping_add(count);
            match header.consumer_reservation.compare_exchange_weak(
                consumer_reservation,
                new_reservation,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some((consumer_reservation, count));
                }
                Err(current) => {
                    consumer_reservation = current;
                }
            }
        }
    }

    /// Creates a new shared queue from a header pointer and region.
    ///
    /// # Safety
    /// - `region` must back the allocation at `header`.
    /// - `header` must be non-null and properly aligned.
    unsafe fn from_header(
        region: Arc<MappedRegion>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        let header_ref = unsafe { header.as_ref() };
        let buffer_mask = header_ref.buffer_mask as usize;
        let buffer_size_in_items = buffer_mask.wrapping_add(1);
        if !buffer_size_in_items.is_power_of_two()
            || buffer_size_in_items == 0
            || SharedQueueHeader::calculate_buffer_size_in_items::<T>(region.file_size())?
                != buffer_size_in_items
        {
            return Err(Error::InvalidBufferSize);
        }

        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - allocation at `header` is large enough to hold the header and the buffer.
        let buffer = unsafe { Self::buffer_from_header(header) };
        Ok(Self {
            header,
            buffer,
            region,
            buffer_mask,
        })
    }

    /// Gets a pointer to the buffer following the header.
    ///
    /// # Safety
    /// - The header must be non-null and properly aligned.
    /// - The allocation at `header` must be of sufficient size to hold the
    ///   header and padding bytes to align the trailing buffer of `T`.
    unsafe fn buffer_from_header(header: NonNull<SharedQueueHeader>) -> NonNull<T> {
        let buffer_offset = SharedQueueHeader::buffer_offset::<T>();

        // SAFETY:
        // - buffer_offset will not overflow isize.
        // - header allocation is large enough to accommodate the alignment.
        let aligned_ptr = unsafe { header.byte_add(buffer_offset) };
        aligned_ptr.cast()
    }
}

#[repr(C)]
struct SharedQueueHeader {
    // Cold metadata cacheline.
    magic: AtomicU64,
    version: u32,
    buffer_mask: u32,

    /// Producer reservation cursor.
    ///
    /// Producers atomically advance this with CAS to claim slots, but claimed
    /// writes are not visible to consumers until `producer_publication` is
    /// advanced.
    producer_reservation: CacheAlignedAtomicSize,
    /// Producer publication cursor.
    ///
    /// Producers advance this in-order after filling reserved slots. Consumers
    /// use it to determine how many initialized items are readable.
    producer_publication: CacheAlignedAtomicSize,
    /// Consumer reservation cursor.
    ///
    /// Consumers atomically advance this with CAS to claim readable slots, but
    /// reclaimed capacity is not visible to producers until `consumer_release`
    /// is advanced.
    consumer_reservation: CacheAlignedAtomicSize,
    /// Consumer release cursor.
    ///
    /// Consumers advance this in-order after dropping/reading claimed slots.
    /// Producers use it to determine how much free space is available.
    consumer_release: CacheAlignedAtomicSize,
}

impl SharedQueueHeader {
    fn create<T: Sized>(
        file: &File,
        size: usize,
    ) -> Result<(Arc<MappedRegion>, NonNull<Self>), Error> {
        file.set_len(size as u64)?;

        let buffer_size_in_items = Self::calculate_buffer_size_in_items::<T>(size)?;
        let region = MappedRegion::new(file, size)?;
        let header = region.addr().cast::<Self>();
        // SAFETY: The header is non-null and aligned properly.
        //         Alignment is guaranteed because mmap ensures that the
        //         memory is aligned to the page size, which is sufficient for the
        //         alignment of `SharedQueueHeader`.
        unsafe { Self::initialize(header, buffer_size_in_items) };
        Ok((region, header))
    }

    const fn buffer_offset<T: Sized>() -> usize {
        const {
            assert!(
                core::mem::align_of::<T>() <= crate::shmem::MINIMUM_REGION_ALIGNMENT,
                "types with alignment > MINIMUM_REGION_ALIGNMENT are not supported"
            )
        }

        core::mem::size_of::<Self>().next_multiple_of(core::mem::align_of::<T>())
    }

    const fn calculate_buffer_size_in_items<T: Sized>(file_size: usize) -> Result<usize, Error> {
        const {
            assert!(
                core::mem::size_of::<T>() > 0,
                "zero-sized types are not supported"
            )
        }

        let buffer_offset = Self::buffer_offset::<T>();
        if file_size < buffer_offset {
            return Err(Error::InvalidBufferSize);
        }

        // The buffer size (in units of T) must be a power of two.
        let buffer_size_in_bytes = file_size - buffer_offset;
        let mut buffer_size_in_items = buffer_size_in_bytes / core::mem::size_of::<T>();
        if !buffer_size_in_items.is_power_of_two() {
            // If not a power of two, round down to the previous power of two.
            buffer_size_in_items = buffer_size_in_items.next_power_of_two() >> 1;
            if buffer_size_in_items == 0 {
                return Err(Error::InvalidBufferSize);
            }
        }

        // The buffer mask is stored as u32, so the capacity must fit.
        if buffer_size_in_items > u32::MAX as usize + 1 {
            return Err(Error::InvalidBufferSize);
        }

        Ok(buffer_size_in_items)
    }

    /// Initializes the shared queue header.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - `header` allocation must be large enough to hold the header and the buffer.
    /// - `access` to `header` must be unique when this is called.
    unsafe fn initialize(mut header_ptr: NonNull<Self>, buffer_size_in_items: usize) {
        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header_ptr.as_mut() };
        header.producer_reservation.store(0, Ordering::Release);
        header.producer_publication.store(0, Ordering::Release);
        header.consumer_reservation.store(0, Ordering::Release);
        header.consumer_release.store(0, Ordering::Release);
        header.buffer_mask = u32::try_from(buffer_size_in_items - 1).unwrap();
        header.version = VERSION;
        header.magic.store(MAGIC, Ordering::Release);
    }

    fn join<T: Sized>(file: &File) -> Result<(Arc<MappedRegion>, NonNull<Self>), Error> {
        let file_size = file.metadata()?.len() as usize;
        let region = MappedRegion::new(file, file_size)?;
        let header = region.addr().cast::<Self>();
        {
            // SAFETY: The header is non-null and aligned properly.
            //         Alignment is guaranteed because mmap ensures that the
            //         memory is aligned to the page size, which is sufficient for the
            //         alignment of `SharedQueueHeader`.
            let header = unsafe { header.as_ref() };
            if header.magic.load(Ordering::Acquire) != MAGIC {
                return Err(Error::InvalidMagic);
            }
            if header.version != VERSION {
                return Err(Error::InvalidVersion {
                    expected: VERSION,
                    actual: header.version,
                });
            }
            let buffer_size_in_items = (header.buffer_mask as usize).wrapping_add(1);
            if buffer_size_in_items != Self::calculate_buffer_size_in_items::<T>(file_size)? {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok((region, header))
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this producer.
    unsafe fn publish_producer_publication(header_ptr: NonNull<Self>, start: usize, count: usize) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        while header.producer_publication.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }
        header
            .producer_publication
            .store(start.wrapping_add(count), Ordering::Release);
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this consumer.
    unsafe fn publish_consumer_release(header_ptr: NonNull<Self>, start: usize, count: usize) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        while header.consumer_release.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }
        header
            .consumer_release
            .store(start.wrapping_add(count), Ordering::Release);
    }
}

#[must_use]
pub struct WriteGuard<'a, T> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> WriteGuard<'a, T> {
    /// Returns a mutable reference to the slot.
    ///
    /// # Safety
    /// - T must be be valid for any bytes.
    pub unsafe fn as_mut_ref(&mut self) -> &mut T {
        // SAFETY: The cell was reserved for writing.
        unsafe { self.cell.as_mut() }
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.cell.as_ptr()
    }

    pub fn write(self, value: T) {
        // SAFETY: The cell was reserved for writing.
        unsafe { self.cell.as_ptr().write(value) };
    }
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved producer slot.
        unsafe {
            SharedQueueHeader::publish_producer_publication(self.header, self.start, 1);
        }
    }
}

#[must_use]
pub struct ReadGuard<'a, T> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> ReadGuard<'a, T> {
    pub fn as_ptr(&self) -> *const T {
        // SAFETY: The cell was reserved for reading.
        self.cell.as_ptr()
    }

    pub fn read(self) -> T {
        // SAFETY: The cell was reserved for reading and holds an initialized value.
        unsafe { self.cell.as_ptr().read() }
    }
}

impl<'a, T> AsRef<T> for ReadGuard<'a, T> {
    /// Returns a shared reference to the reserved slot.
    fn as_ref(&self) -> &T {
        // SAFETY: The cell was reserved for reading and is initialized.
        unsafe { self.cell.as_ref() }
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved consumer slot.
        unsafe {
            SharedQueueHeader::publish_consumer_release(self.header, self.start, 1);
        }
    }
}

#[must_use]
pub struct WriteBatch<'a, T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    count: usize,
    buffer_mask: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> WriteBatch<'a, T> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns a mutable reference to the reserved slot.
    ///
    /// # Safety
    /// - The slot is uninitialized; caller must fully initialize `T`.
    /// - `index < count`
    /// - `T` must be valid for any bytes.
    pub unsafe fn as_mut(&mut self, index: usize) -> &mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_mut() }
    }

    /// Returns a mutable pointer to the reserved slot.
    ///
    /// # Safety
    /// - The slot is uninitialized; caller must fully initialize `T`.
    /// - `index < count`
    pub unsafe fn as_mut_ptr(&mut self, index: usize) -> *mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
    }

    /// Writes a value into the slot at index.
    ///
    /// # Safety
    /// - `index < count`
    pub unsafe fn write(&mut self, index: usize, value: T) {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing
        unsafe { self.buffer.add(position & self.buffer_mask).write(value) }
    }
}

impl<'a, T> Drop for WriteBatch<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved producer slots.
        unsafe {
            SharedQueueHeader::publish_producer_publication(self.header, self.start, self.count);
        }
    }
}

#[must_use]
pub struct ReadBatch<'a, T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    count: usize,
    buffer_mask: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> ReadBatch<'a, T> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns a reference to the reserved slot.
    ///
    /// # Safety
    /// - `index` must be less than `self.len()`
    pub unsafe fn as_ref(&self, index: usize) -> &T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for reading and is initialized.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ref() }
    }

    /// Returns a pointer to the reserved slot.
    ///
    /// # Safety
    /// - `index` must be less than `self.len()`
    pub unsafe fn as_ptr(&self, index: usize) -> *const T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for reading.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
    }

    /// Read the value at index
    ///
    /// # Safety
    /// - `index` must be less than `self.len()`
    pub unsafe fn read(&self, index: usize) -> T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for reading.
        unsafe { self.buffer.add(position & self.buffer_mask).read() }
    }
}

impl<'a, T> Drop for ReadBatch<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved consumer slots.
        unsafe {
            SharedQueueHeader::publish_consumer_release(self.header, self.start, self.count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::create_temp_shmem_file;

    type Item = u64;
    const BUFFER_CAPACITY: usize = 512;
    const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);

    fn create_test_queue<T: Sized>(file_size: usize) -> (File, Producer<T>, Consumer<T>) {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::create(&file, file_size) }.expect("Failed to create producer");
        let consumer = unsafe { Consumer::join(&file) }.expect("Failed to join consumer");

        (file, producer, consumer)
    }

    #[test]
    fn test_producer_consumer() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let capacity =
            SharedQueueHeader::calculate_buffer_size_in_items::<Item>(BUFFER_SIZE).unwrap();

        for i in 0..capacity {
            assert_eq!(producer.try_write(i as Item), Ok(()));
        }
        assert!(producer.try_write(999).is_err());

        for i in 0..capacity {
            assert_eq!(consumer.try_read(), Some(i as Item));
        }
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_reserve_and_try_read_ptr() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut guard = unsafe { producer.reserve_write() }.expect("reserve failed");
        unsafe {
            *guard.as_mut_ptr() = 42;
        }
        drop(guard);

        let guard = consumer.reserve_read().expect("try_read_ptr failed");
        unsafe {
            assert_eq!(*guard.as_ptr(), 42);
        }
        assert_eq!(*guard.as_ref(), 42);
    }

    #[test]
    fn test_reserve_batch_and_try_read_batch() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut batch = unsafe { producer.reserve_write_batch(4) }.expect("reserve_batch failed");
        for index in 0..batch.len() {
            unsafe {
                *batch.as_mut_ptr(index) = index as u64;
            }
        }
        drop(batch);

        let batch = consumer
            .reserve_read_batch(4)
            .expect("try_read_batch failed");
        for index in 0..batch.len() {
            unsafe {
                assert_eq!(*batch.as_ptr(index), index as u64);
            }
            assert_eq!(unsafe { batch.read(index) }, index as u64);
        }
    }

    #[test]
    fn test_batch_write_exact_read_upto_max() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        unsafe {
            assert!(producer.reserve_write_batch(0).is_none());
            assert!(producer.reserve_write_batch(BUFFER_CAPACITY + 1).is_none());
        }

        for i in 0..4 {
            assert_eq!(producer.try_write(i as Item), Ok(()));
        }
        let batch = consumer
            .reserve_read_batch(5)
            .expect("try_read_batch up-to failed");
        assert_eq!(batch.len(), 4);
        for index in 0..batch.len() {
            // SAFETY: `batch` has exactly 4 readable items.
            unsafe {
                assert_eq!(*batch.as_ptr(index), index as u64);
            }
        }
    }

    #[test]
    fn test_try_write_slice() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        assert!(producer.try_write_slice(&[]));

        let values = [10, 11, 12, 13];
        assert!(producer.try_write_slice(&values));
        for value in values {
            assert_eq!(consumer.try_read(), Some(value));
        }
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_minimum_file_size_rounds_up_capacity() {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe { Producer::<u64>::create(&file, minimum_file_size::<u64>(3)) }
            .expect("create failed");
        let consumer = unsafe { Consumer::<u64>::join(&file) }.expect("join failed");

        assert_eq!(producer.queue.capacity(), 4);
        assert_eq!(consumer.queue.capacity(), 4);
    }

    #[test]
    fn test_multiple_producers_consumers() {
        let (file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = unsafe { Producer::join(&file) }.expect("Failed to create producer2");
        let consumer2 = unsafe { Consumer::join(&file) }.expect("Failed to create consumer2");

        let capacity = BUFFER_CAPACITY;
        for i in 0..(capacity / 2) {
            assert_eq!(producer.try_write((i * 2) as Item), Ok(()));
            assert_eq!(producer2.try_write((i * 2 + 1) as Item), Ok(()));
        }

        let mut values = Vec::with_capacity(capacity);
        while values.len() < capacity {
            let mut progressed = false;
            if let Some(value) = consumer.try_read() {
                values.push(value);
                progressed = true;
            }
            if let Some(value) = consumer2.try_read() {
                values.push(value);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }

        assert_eq!(values.len(), capacity);
        values.sort_unstable();
        for (i, value) in values.iter().enumerate() {
            assert_eq!(*value, i as Item);
        }
    }

    #[test]
    fn test_clone_producer() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.clone();

        producer.try_write(10).unwrap();
        producer2.try_write(20).unwrap();

        let mut values = Vec::new();
        while let Some(v) = consumer.try_read() {
            values.push(v);
        }
        values.sort_unstable();
        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn test_clone_consumer() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let consumer2 = consumer.clone();

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let mut values = Vec::new();
        loop {
            let mut progressed = false;
            if let Some(v) = consumer.try_read() {
                values.push(v);
                progressed = true;
            }
            if let Some(v) = consumer2.try_read() {
                values.push(v);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        values.sort_unstable();
        assert_eq!(values, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_cross_role_joins() {
        let (_file, producer1, consumer1) = create_test_queue::<Item>(BUFFER_SIZE);
        let consumer2 = producer1.join_as_consumer();
        let producer2 = consumer2.join_as_producer();

        // Write two values.
        producer1.try_write(100).unwrap();
        producer2.try_write(200).unwrap();

        // Read two values.
        assert_eq!(consumer2.try_read().unwrap(), 100);
        assert_eq!(consumer1.try_read().unwrap(), 200);
    }

    #[test]
    fn test_drop_original_mapping_stays_alive() {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<Item>::create(&file, BUFFER_SIZE) }.expect("create failed");
        let consumer = producer.join_as_consumer();
        let producer2 = producer.clone();

        // Drop the original producer — the mapping stays alive via Arc.
        drop(producer);

        producer2.try_write(42).unwrap();
        assert_eq!(consumer.try_read(), Some(42));
    }
}
