//! DPDK-style bounded broadcast ring queue.
//!
//! This queue mirrors the producer-side reservation/publication flow of the
//! MPMC queue, but consumers do not reserve shared slots. Instead each
//! consumer tracks its own local cursor and copies values out of the ring.
//! Reads fail with a skipped-item count if the consumer falls behind and
//! producers wrap around the ring before the copied values can be validated.

use crate::{
    error::Error, normalized_capacity, shmem::MappedRegion, CacheAlignedAtomicSize, VERSION,
};
use core::{marker::PhantomData, ptr::NonNull, sync::atomic::Ordering};
use std::{
    fs::File,
    sync::{atomic::AtomicU64, Arc},
};

/// Unique identifier for broadcast queue in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");

pub struct Producer<T: Copy> {
    queue: SharedQueue<T>,
}

impl<T: Copy> Producer<T> {
    /// Creates a new producer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The provided file must be uniquely created as a Producer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or copy a queued value,
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
    /// - If a process may read, dereference, mutate, or copy a queued value,
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
        Consumer::from_queue(self.queue.clone())
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

    /// Writes item into the queue.
    pub fn try_write(&self, item: T) {
        // SAFETY: On successful reservation the item is written below.
        let guard = unsafe { self.reserve_write() }.expect("single-slot reservation must succeed");
        guard.write(item);
    }

    /// Writes items from a slice into the queue.
    ///
    /// Returns `false` if the slice exceeds the queue capacity.
    pub fn try_write_slice(&self, items: &[T]) -> bool {
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

impl<T: Copy> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

unsafe impl<T: Copy> Send for Producer<T> {}
unsafe impl<T: Copy> Sync for Producer<T> {}

pub struct Consumer<T: Copy> {
    queue: SharedQueue<T>,
    next: usize,
}

impl<T: Copy> Consumer<T> {
    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The provided file must be uniquely created as a Consumer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or copy a queued value,
    ///   that operation must be valid for that value in that process.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::create::<T>(file, file_size)?;
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Ok(Self::from_queue(queue))
    }

    /// Joins an existing consumer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or copy a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Producer`]s that are joined with
    ///   the same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Ok(Self::from_queue(queue))
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> Producer<T> {
        Producer {
            queue: self.queue.clone(),
        }
    }

    fn from_queue(queue: SharedQueue<T>) -> Self {
        let next = queue.oldest_available();
        Self { queue, next }
    }

    /// Repositions the consumer to the oldest item still retained in the ring.
    ///
    /// This is useful after an overrun when the consumer wants to resume
    /// ordered reads from the earliest value that has not yet been overwritten.
    pub fn sync_to_oldest(&mut self) {
        self.next = self.queue.oldest_available();
    }

    /// Repositions the consumer to the producer publication cursor.
    ///
    /// After this call the consumer will ignore any currently buffered items
    /// and only observe values published afterwards.
    pub fn sync_to_latest(&mut self) {
        self.next = self.queue.published();
    }

    /// Attempts to read a value from the queue.
    /// Returns `Ok(None)` if there are no values available.
    pub fn try_read(&mut self) -> Result<Option<T>, usize> {
        let start = self.next;
        let published = self.queue.published();
        let available = published.wrapping_sub(start);
        if available == 0 {
            return Ok(None);
        }
        if available > self.queue.capacity() {
            let overrun = self.queue.overrun(start, published);
            self.next = published.wrapping_sub(self.queue.capacity());
            return Err(overrun);
        }

        let item = self.queue.read_copy(start);
        let published_after = self.queue.published();
        if published_after.wrapping_sub(start) > self.queue.capacity() {
            let overrun = self.queue.overrun(start, published_after);
            self.next = published_after.wrapping_sub(self.queue.capacity());
            return Err(overrun);
        }

        self.next = start.wrapping_add(1);
        Ok(Some(item))
    }

    /// Attempts to read a value from the queue without copying it.
    ///
    /// The returned guard exposes references into shared memory. Producers may
    /// overwrite that memory concurrently, so the caller must treat all access
    /// through the guard as unsafe and call [`DirectRead::validate`] before
    /// trusting any work derived from the referenced value.
    ///
    /// # Safety
    /// Any references obtained from the returned guard may be invalidated by
    /// concurrent producers before the caller validates the guard. The caller
    /// must not trust data derived from those references unless
    /// [`DirectRead::validate`] or [`DirectRead::commit`] succeeds afterwards.
    pub unsafe fn try_read_direct(&mut self) -> Result<Option<DirectRead<'_, T>>, usize> {
        let start = self.next;
        let published = self.queue.published();
        let available = published.wrapping_sub(start);
        if available == 0 {
            return Ok(None);
        }
        if available > self.queue.capacity() {
            let overrun = self.queue.overrun(start, published);
            self.next = published.wrapping_sub(self.queue.capacity());
            return Err(overrun);
        }

        Ok(Some(DirectRead {
            next: &mut self.next,
            queue: &self.queue,
            start,
        }))
    }

    /// Attempts to read up to `out.len()` values from the queue into `out`.
    ///
    /// On success this returns the initialized prefix of `out` as an immutable
    /// slice. On overrun the caller must treat the contents of `out[..]`
    /// as invalid.
    pub fn try_read_batch<'a>(&mut self, out: &'a mut [T]) -> Result<&'a [T], usize> {
        if out.is_empty() {
            return Ok(&out[..0]);
        }

        let start = self.next;
        let published = self.queue.published();
        let available = published.wrapping_sub(start);
        if available == 0 {
            return Ok(&out[..0]);
        }
        if available > self.queue.capacity() {
            let overrun = self.queue.overrun(start, published);
            self.next = published.wrapping_sub(self.queue.capacity());
            return Err(overrun);
        }

        let count = available.min(out.len());
        self.queue.read_copy_batch(start, &mut out[..count]);
        let published_after = self.queue.published();
        if published_after.wrapping_sub(start) > self.queue.capacity() {
            let overrun = self.queue.overrun(start, published_after);
            self.next = published_after.wrapping_sub(self.queue.capacity());
            return Err(overrun);
        }

        self.next = start.wrapping_add(count);
        Ok(&out[..count])
    }

    /// Attempts to read up to `max` values from the queue without copying them.
    ///
    /// The returned guard exposes references into shared memory. Producers may
    /// overwrite that memory concurrently, so the caller must treat all access
    /// through the guard as unsafe and call [`DirectReadBatch::validate`] before
    /// trusting any work derived from the referenced values.
    ///
    /// # Safety
    /// Any references obtained from the returned guard may be invalidated by
    /// concurrent producers before the caller validates the guard. The caller
    /// must not trust data derived from those references unless
    /// [`DirectReadBatch::validate`] or [`DirectReadBatch::commit`] succeeds
    /// afterwards.
    pub unsafe fn try_read_direct_batch(
        &mut self,
        max: usize,
    ) -> Result<Option<DirectReadBatch<'_, T>>, usize> {
        if max == 0 {
            return Ok(None);
        }

        let start = self.next;
        let published = self.queue.published();
        let available = published.wrapping_sub(start);
        if available == 0 {
            return Ok(None);
        }
        if available > self.queue.capacity() {
            let overrun = self.queue.overrun(start, published);
            self.next = published.wrapping_sub(self.queue.capacity());
            return Err(overrun);
        }

        let count = available.min(max);
        Ok(Some(DirectReadBatch {
            next: &mut self.next,
            queue: &self.queue,
            start,
            count,
        }))
    }

    /// Copies the most recently published retained value.
    pub fn try_read_latest(&mut self) -> Result<Option<T>, usize> {
        let published = self.queue.published();
        let oldest = self.queue.oldest_available();
        let retained = published.wrapping_sub(oldest);
        if retained == 0 {
            self.next = published;
            return Ok(None);
        }

        let start = published.wrapping_sub(1);
        let item = self.queue.read_copy(start);
        let published_after = self.queue.published();
        if published_after.wrapping_sub(start) > self.queue.capacity() {
            self.next = published_after;
            return Err(self.queue.overrun(start, published_after));
        }

        self.next = published;
        Ok(Some(item))
    }

    /// Copies up to `out.len()` of the most recently published retained values.
    ///
    /// Values are copied into `out[..count]` in publication order. The consumer
    /// cursor is advanced to the current publication cursor, so subsequent
    /// ordered reads only observe newer values.
    pub fn try_read_latest_batch<'a>(&mut self, out: &'a mut [T]) -> Result<&'a [T], usize> {
        if out.is_empty() {
            return Ok(&out[..0]);
        }

        let published = self.queue.published();
        let oldest = self.queue.oldest_available();
        let retained = published.wrapping_sub(oldest);
        if retained == 0 {
            self.next = published;
            return Ok(&out[..0]);
        }

        let count = retained.min(out.len());
        let start = published.wrapping_sub(count);
        self.queue.read_copy_batch(start, &mut out[..count]);
        let published_after = self.queue.published();
        if published_after.wrapping_sub(start) > self.queue.capacity() {
            self.next = published_after;
            return Err(self.queue.overrun(start, published_after));
        }

        self.next = published;
        Ok(&out[..count])
    }

    /// Backward-compatible alias for ordered batched reads.
    pub fn try_read_slice<'a>(&mut self, out: &'a mut [T]) -> Result<&'a [T], usize> {
        self.try_read_batch(out)
    }
}

impl<T: Copy> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            next: self.next,
        }
    }
}

unsafe impl<T: Copy> Send for Consumer<T> {}
unsafe impl<T: Copy> Sync for Consumer<T> {}

/// Calculates the minimum file size required for a queue with given capacity.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
pub const fn minimum_file_size<T: Sized>(capacity: usize) -> usize {
    let buffer_offset = SharedQueueHeader::buffer_offset::<T>();
    buffer_offset + normalized_capacity(capacity) * core::mem::size_of::<T>()
}

struct SharedQueue<T: Copy> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    buffer_mask: usize,

    // NB: Region must be declared last so it is dropped last ensuring `header` and
    // `buffer` remain valid for their entire lifetime.
    region: Arc<MappedRegion>,
}

impl<T: Copy> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            header: self.header,
            buffer: self.buffer,
            buffer_mask: self.buffer_mask,
            region: Arc::clone(&self.region),
        }
    }
}

impl<T: Copy> SharedQueue<T> {
    #[inline]
    fn overrun(&self, start: usize, published: usize) -> usize {
        published.wrapping_sub(start).wrapping_sub(self.capacity())
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    #[inline]
    fn published(&self) -> usize {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        unsafe { self.header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire)
    }

    #[inline]
    fn oldest_available(&self) -> usize {
        let published = self.published();
        published.saturating_sub(self.capacity())
    }

    fn reserve_write(&self) -> Option<(NonNull<T>, usize)> {
        let position = self.reserve_write_batch(1)?;
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

    #[inline]
    fn read_copy(&self, position: usize) -> T {
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds. `T: Copy` so duplicating
        // the value does not transfer ownership out of shared memory.
        unsafe { self.buffer.add(cell_index).as_ptr().read() }
    }

    #[inline]
    fn ptr_at(&self, position: usize) -> *const T {
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        unsafe { self.buffer.add(cell_index).as_ptr() }
    }

    fn read_copy_batch(&self, start: usize, out: &mut [T]) {
        if out.is_empty() {
            return;
        }

        let first = (self.capacity() - (start & self.buffer_mask)).min(out.len());
        // SAFETY: the first range is within bounds of both source and destination.
        unsafe {
            self.buffer
                .add(start & self.buffer_mask)
                .as_ptr()
                .copy_to_nonoverlapping(out.as_mut_ptr(), first);
        }
        if first == out.len() {
            return;
        }

        // SAFETY: the wrapped second range is within bounds of both buffers.
        unsafe {
            self.buffer
                .as_ptr()
                .copy_to_nonoverlapping(out.as_mut_ptr().add(first), out.len() - first);
        }
    }

    #[inline]
    fn validate_window(&self, start: usize, count: usize) -> Result<(), usize> {
        debug_assert!(count <= self.capacity());
        let published = self.published();
        if published.wrapping_sub(start) > self.capacity() {
            return Err(self.overrun(start, published));
        }
        if published.wrapping_sub(start.wrapping_add(count)) > self.capacity() {
            return Err(self.overrun(start, published));
        }
        Ok(())
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
    /// use it to determine which values are currently retained in the ring.
    producer_publication: CacheAlignedAtomicSize,
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
}

#[must_use]
pub struct WriteGuard<'a, T: Copy> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T: Copy> WriteGuard<'a, T> {
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

impl<'a, T: Copy> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved producer slot.
        unsafe {
            SharedQueueHeader::publish_producer_publication(self.header, self.start, 1);
        }
    }
}

#[must_use]
pub struct WriteBatch<'a, T: Copy> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    count: usize,
    buffer_mask: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T: Copy> WriteBatch<'a, T> {
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
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).write(value) }
    }
}

impl<'a, T: Copy> Drop for WriteBatch<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved producer slots.
        unsafe {
            SharedQueueHeader::publish_producer_publication(self.header, self.start, self.count);
        }
    }
}

#[must_use]
pub struct DirectRead<'a, T: Copy> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T>,
    start: usize,
}

impl<'a, T: Copy> DirectRead<'a, T> {
    pub fn as_ptr(&self) -> *const T {
        self.queue.ptr_at(self.start)
    }

    /// Returns a shared reference into the broadcast ring.
    ///
    /// # Safety
    /// The returned reference points into shared memory that may be overwritten
    /// by producers at any time. The caller must validate the guard after using
    /// the reference and discard any derived results if validation fails.
    pub unsafe fn as_ref(&self) -> &T {
        // SAFETY: caller upholds the shared-memory lifetime requirements.
        unsafe { &*self.as_ptr() }
    }

    pub fn validate(&self) -> Result<(), usize> {
        self.queue.validate_window(self.start, 1)
    }

    pub fn commit(self) -> Result<(), usize> {
        self.validate()?;
        *self.next = self.start.wrapping_add(1);
        Ok(())
    }
}

#[must_use]
pub struct DirectReadBatch<'a, T: Copy> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T>,
    start: usize,
    count: usize,
}

impl<'a, T: Copy> DirectReadBatch<'a, T> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns a shared reference into the broadcast ring.
    ///
    /// # Safety
    /// The returned reference points into shared memory that may be overwritten
    /// by producers at any time. The caller must validate the guard after using
    /// the reference and discard any derived results if validation fails.
    /// `index` must be less than [`Self::len`].
    pub unsafe fn as_ref(&self, index: usize) -> &T {
        // SAFETY: caller upholds the shared-memory lifetime requirements.
        unsafe { &*self.as_ptr(index) }
    }

    /// Returns a pointer into the broadcast ring.
    ///
    /// # Safety
    /// `index` must be less than [`Self::len`].
    pub unsafe fn as_ptr(&self, index: usize) -> *const T {
        debug_assert!(index < self.count);
        self.queue.ptr_at(self.start.wrapping_add(index))
    }

    pub fn validate(&self) -> Result<(), usize> {
        self.queue.validate_window(self.start, self.count)
    }

    pub fn commit(self) -> Result<(), usize> {
        self.validate()?;
        *self.next = self.start.wrapping_add(self.count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::create_temp_shmem_file;

    type Item = u64;
    const BUFFER_CAPACITY: usize = 8;
    const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);

    fn create_test_queue<T: Copy>(file_size: usize) -> (File, Producer<T>, Consumer<T>) {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::create(&file, file_size) }.expect("Failed to create producer");
        let consumer = unsafe { Consumer::join(&file) }.expect("Failed to join consumer");

        (file, producer, consumer)
    }

    #[test]
    fn test_producer_consumer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..BUFFER_CAPACITY {
            producer.try_write(i as Item);
        }

        for i in 0..BUFFER_CAPACITY {
            assert_eq!(consumer.try_read(), Ok(Some(i as Item)));
        }
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_try_write_slice_and_read_slice() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let values = [10, 11, 12, 13];
        assert!(producer.try_write_slice(&values));

        let mut out = [0; 4];
        let read = consumer.try_read_slice(&mut out).unwrap();
        assert_eq!(read, values);
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_reserve_batch_and_read_slice() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut batch = unsafe { producer.reserve_write_batch(4) }.expect("reserve_batch failed");
        for index in 0..batch.len() {
            unsafe {
                *batch.as_mut_ptr(index) = index as u64;
            }
        }
        drop(batch);

        let mut out = [0; 4];
        let read = consumer.try_read_batch(&mut out).unwrap();
        assert_eq!(read, [0, 1, 2, 3]);
    }

    #[test]
    fn test_multiple_consumers_receive_all_values() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let mut consumer2 = consumer.clone();

        for i in 0..4 {
            producer.try_write(i);
        }

        let mut values1 = Vec::new();
        let mut values2 = Vec::new();
        while let Some(v) = consumer.try_read().unwrap() {
            values1.push(v);
        }
        while let Some(v) = consumer2.try_read().unwrap() {
            values2.push(v);
        }

        assert_eq!(values1, vec![0, 1, 2, 3]);
        assert_eq!(values2, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_overrun_repositions_consumer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..(BUFFER_CAPACITY as u64 + 2) {
            producer.try_write(i);
        }

        assert_eq!(consumer.try_read(), Err(2));
        for expected in 2..(BUFFER_CAPACITY as u64 + 2) {
            assert_eq!(consumer.try_read(), Ok(Some(expected)));
        }
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_sync_modes() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            producer.try_write(i);
        }

        consumer.sync_to_latest();
        assert_eq!(consumer.try_read(), Ok(None));

        for i in 4..8 {
            producer.try_write(i);
        }

        consumer.sync_to_oldest();
        for expected in 0..8 {
            assert_eq!(consumer.try_read(), Ok(Some(expected)));
        }
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_try_read_latest_returns_latest_retained_values_in_order() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..(BUFFER_CAPACITY as u64 + 4) {
            producer.try_write(i);
        }

        let mut out = [0; 3];
        let read = consumer.try_read_latest_batch(&mut out).unwrap();
        assert_eq!(read, [9, 10, 11]);
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_try_read_latest_returns_most_recent_item() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..5 {
            producer.try_write(i);
        }

        assert_eq!(consumer.try_read_latest(), Ok(Some(4)));
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_try_read_direct_reads_without_copy() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(42);

        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        unsafe {
            assert_eq!(*direct.as_ref(), 42);
        }
        assert_eq!(direct.validate(), Ok(()));
        assert_eq!(direct.commit(), Ok(()));
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_try_read_direct_batch_reads_without_copy() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            producer.try_write(i);
        }

        let direct = unsafe { consumer.try_read_direct_batch(8) }
            .unwrap()
            .unwrap();
        assert_eq!(direct.len(), 4);
        for index in 0..direct.len() {
            unsafe {
                assert_eq!(*direct.as_ref(index), index as u64);
            }
        }
        assert_eq!(direct.validate(), Ok(()));
        assert_eq!(direct.commit(), Ok(()));
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_try_read_direct_detects_overrun_after_access() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(1);
        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        unsafe {
            let _ = *direct.as_ref();
        }

        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(10 + i);
        }

        assert_eq!(direct.validate(), Err(1));
        assert_eq!(direct.commit(), Err(1));
    }

    #[test]
    fn test_join_consumer_starts_at_oldest_retained() {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<Item>::create(&file, BUFFER_SIZE) }.expect("create failed");

        for i in 0..(BUFFER_CAPACITY as u64 + 3) {
            producer.try_write(i);
        }

        let mut consumer = unsafe { Consumer::<Item>::join(&file) }.expect("join failed");
        for expected in 3..(BUFFER_CAPACITY as u64 + 3) {
            assert_eq!(consumer.try_read(), Ok(Some(expected)));
        }
        assert_eq!(consumer.try_read(), Ok(None));
    }

    #[test]
    fn test_clone_producer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.clone();

        producer.try_write(10);
        producer2.try_write(20);

        let mut values = Vec::new();
        while let Some(v) = consumer.try_read().unwrap() {
            values.push(v);
        }
        values.sort_unstable();
        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn test_cross_role_joins() {
        let (_file, producer1, mut consumer1) = create_test_queue::<Item>(BUFFER_SIZE);
        let mut consumer2 = producer1.join_as_consumer();
        let producer2 = consumer2.join_as_producer();

        producer1.try_write(100);
        producer2.try_write(200);

        assert_eq!(consumer1.try_read().unwrap(), Some(100));
        assert_eq!(consumer1.try_read().unwrap(), Some(200));
        assert_eq!(consumer2.try_read().unwrap(), Some(100));
        assert_eq!(consumer2.try_read().unwrap(), Some(200));
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
}
