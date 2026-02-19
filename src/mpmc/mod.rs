//! DPDK-style bounded MPMC ring queue

use crate::{error::Error, shmem::map_file, CacheAlignedAtomicSize, VERSION};
use core::{
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::{AtomicU8, Ordering},
};
use std::fs::File;

pub struct Producer<T> {
    queue: SharedQueue<T>,
}

impl<T> Producer<T> {
    /// Creates a new producer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The provided file must be uniquely created as a Producer.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        let header = SharedQueueHeader::create::<T>(file, file_size)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(header, file_size) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    pub fn join(file: &File) -> Result<Self, Error> {
        let (header, file_size) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(header, file_size) }
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation at `header` must be of size `file_size` or greater.
    unsafe fn from_header(
        header: NonNull<SharedQueueHeader>,
        file_size: usize,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - allocation at `header` is large enough to hold the header and the buffer.
            queue: unsafe { SharedQueue::from_header(header, file_size) }?,
        })
    }

    /// Writes item into the queue or returns it if there is not enough space.
    pub fn try_write(&self, item: T) -> Result<(), T> {
        // SAFETY: On successful reservation the item is written below.
        let guard = match unsafe { self.reserve() } {
            Some(guard) => guard,
            None => return Err(item),
        };
        guard.write(item);
        Ok(())
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
    pub unsafe fn reserve(&self) -> Option<WriteGuard<'_, T>> {
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
    pub unsafe fn reserve_batch(&self, count: usize) -> Option<WriteBatch<'_, T>> {
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

unsafe impl<T> Send for Producer<T> {}

pub struct Consumer<T> {
    queue: SharedQueue<T>,
}

impl<T> Consumer<T> {
    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The provided file must be uniquely created as a Consumer.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        let header = SharedQueueHeader::create::<T>(file, file_size)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(header, file_size) }
    }

    /// Joins an existing consumer for the shared queue in the provided file.
    pub fn join(file: &File) -> Result<Self, Error> {
        let (header, file_size) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(header, file_size) }
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation at `header` must be of size `file_size` or greater.
    unsafe fn from_header(
        header: NonNull<SharedQueueHeader>,
        file_size: usize,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - allocation at `header` is large enough to hold the header and the buffer.
            queue: unsafe { SharedQueue::from_header(header, file_size) }?,
        })
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    pub fn try_read(&self) -> Option<T> {
        self.try_read_ptr().map(ReadGuard::read)
    }

    /// Attempts to reserve a value from the queue, returning a guard.
    /// The slot is released back to producers when the guard is dropped.
    ///
    /// Other [`Consumer`]s may read in parallel, but reads must be
    /// released in order they were reserved. Holding a [`ReadGuard`] should
    /// be treated similarly to holding a lock on a critical section.
    #[must_use]
    pub fn try_read_ptr(&self) -> Option<ReadGuard<'_, T>> {
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
    pub fn try_read_batch(&self, max: usize) -> Option<ReadBatch<'_, T>> {
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

unsafe impl<T> Send for Consumer<T> {}

/// Calculates the minimum file size required for a queue with given capacity.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
pub const fn minimum_file_size<T: Sized>(capacity: usize) -> usize {
    let buffer_offset = SharedQueueHeader::buffer_offset::<T>();
    buffer_offset + capacity * core::mem::size_of::<T>()
}

#[repr(C)]
struct SharedQueue<T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    file_size: usize,
    buffer_mask: usize,
}

impl<T> Drop for SharedQueue<T> {
    fn drop(&mut self) {
        // Tests do not mmap so skip unmapping in tests.
        #[cfg(test)]
        {
            return;
        }

        #[allow(unreachable_code)]
        // SAFETY: header is mmapped and of size `file_size`.
        unsafe {
            libc::munmap(self.header.as_ptr().cast(), self.file_size);
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

    /// Creates a new shared queue from a header pointer and file size.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation at `header` must be of size `file_size`.
    unsafe fn from_header(
        header: NonNull<SharedQueueHeader>,
        file_size: usize,
    ) -> Result<Self, Error> {
        let header_ref = unsafe { header.as_ref() };
        let buffer_mask = header_ref.buffer_mask;
        let buffer_size_in_items = buffer_mask.wrapping_add(1);
        if !buffer_size_in_items.is_power_of_two()
            || buffer_size_in_items == 0
            || SharedQueueHeader::calculate_buffer_size_in_items::<T>(file_size)?
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
            file_size,
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
    buffer_mask: usize,
    version: AtomicU8,
}

impl SharedQueueHeader {
    fn create<T: Sized>(file: &File, size: usize) -> Result<NonNull<Self>, Error> {
        file.set_len(size as u64)?;

        let buffer_size_in_items = Self::calculate_buffer_size_in_items::<T>(size)?;
        let header = map_file(file, size)?.cast::<Self>();
        // SAFETY: The header is non-null and aligned properly.
        //         Alignment is guaranteed because `create_and_map_file` will return
        //         a pointer only if mapping was successful. mmap ensures that the
        //         memory is aligned to the page size, which is sufficient for the
        //         alignment of `SharedQueueHeader`.
        unsafe { Self::initialize(header, buffer_size_in_items) };
        Ok(header)
    }

    const fn buffer_offset<T: Sized>() -> usize {
        core::mem::size_of::<Self>().next_multiple_of(core::mem::align_of::<T>())
    }

    const fn calculate_buffer_size_in_items<T: Sized>(file_size: usize) -> Result<usize, Error> {
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
        header.buffer_mask = buffer_size_in_items - 1;
        header.version.store(VERSION, Ordering::SeqCst);
    }

    fn join<T: Sized>(file: &File) -> Result<(NonNull<Self>, usize), Error> {
        let file_size = file.metadata()?.len() as usize;
        let header = map_file(file, file_size)?.cast::<Self>();
        {
            // SAFETY: The header is non-null and aligned properly.
            //         Alignment is guaranteed because `open_and_map_file` will return
            //         a pointer only if mapping was successful. mmap ensures that the
            //         memory is aligned to the page size, which is sufficient for the
            //         alignment of `SharedQueueHeader`.
            let header = unsafe { header.as_ref() };
            if header.version.load(Ordering::SeqCst) != VERSION {
                return Err(Error::InvalidVersion);
            }
            let buffer_size_in_items = header.buffer_mask.wrapping_add(1);
            if buffer_size_in_items != Self::calculate_buffer_size_in_items::<T>(file_size)? {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok((header, file_size))
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
    pub fn as_mut_ptr(&mut self) -> *mut T {
        // SAFETY: The cell was reserved for writing.
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
    /// The slot is uninitialized; caller must fully initialize `T`.
    pub unsafe fn as_mut(&mut self, index: usize) -> &mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_mut() }
    }

    pub fn as_mut_ptr(&mut self, index: usize) -> *mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
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

    pub fn as_ptr(&self, index: usize) -> *const T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for reading.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
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
    use crate::aligned_buffer::AlignedBuffer;

    type Item = u64;
    const BUFFER_CAPACITY: usize = 512;
    const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);

    fn create_test_queue<T: Sized>(buffer: &mut [u8]) -> (Producer<T>, Consumer<T>) {
        let file_size = buffer.len();
        let buffer_size_in_items =
            SharedQueueHeader::calculate_buffer_size_in_items::<T>(file_size)
                .expect("Invalid buffer size");
        let header = NonNull::new(buffer.as_mut_ptr().cast()).expect("Failed to create header");
        unsafe { SharedQueueHeader::initialize(header, buffer_size_in_items) };

        (
            unsafe { Producer::from_header(header, file_size) }.expect("Failed to create producer"),
            unsafe { Consumer::from_header(header, file_size) }.expect("Failed to create consumer"),
        )
    }

    #[test]
    fn test_producer_consumer() {
        let mut buffer = AlignedBuffer([0u8; BUFFER_SIZE]);
        let (producer, consumer) = create_test_queue::<Item>(&mut buffer.0);
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
        let mut buffer = AlignedBuffer([0u8; BUFFER_SIZE]);
        let (producer, consumer) = create_test_queue::<Item>(&mut buffer.0);

        let mut guard = unsafe { producer.reserve() }.expect("reserve failed");
        unsafe {
            *guard.as_mut_ptr() = 42;
        }
        drop(guard);

        let guard = consumer.try_read_ptr().expect("try_read_ptr failed");
        unsafe {
            assert_eq!(*guard.as_ptr(), 42);
        }
    }

    #[test]
    fn test_reserve_batch_and_try_read_batch() {
        let mut buffer = AlignedBuffer([0u8; BUFFER_SIZE]);
        let (producer, consumer) = create_test_queue::<Item>(&mut buffer.0);

        let mut batch = unsafe { producer.reserve_batch(4) }.expect("reserve_batch failed");
        for index in 0..batch.len() {
            unsafe {
                *batch.as_mut_ptr(index) = index as u64;
            }
        }
        drop(batch);

        let batch = consumer.try_read_batch(4).expect("try_read_batch failed");
        for index in 0..batch.len() {
            unsafe {
                assert_eq!(*batch.as_ptr(index), index as u64);
            }
        }
    }

    #[test]
    fn test_batch_write_exact_read_upto_max() {
        let mut buffer = AlignedBuffer([0u8; BUFFER_SIZE]);
        let (producer, consumer) = create_test_queue::<Item>(&mut buffer.0);

        unsafe {
            assert!(producer.reserve_batch(0).is_none());
            assert!(producer.reserve_batch(BUFFER_CAPACITY + 1).is_none());
        }

        for i in 0..4 {
            assert_eq!(producer.try_write(i as Item), Ok(()));
        }
        let batch = consumer
            .try_read_batch(5)
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
    fn test_multiple_producers_consumers() {
        let mut buffer = AlignedBuffer([0u8; BUFFER_SIZE]);

        let file_size = buffer.0.len();
        let buffer_size_in_items =
            SharedQueueHeader::calculate_buffer_size_in_items::<Item>(file_size)
                .expect("Invalid buffer size");
        let header = NonNull::new(buffer.0.as_mut_ptr().cast()).expect("Failed to create header");
        unsafe { SharedQueueHeader::initialize(header, buffer_size_in_items) };

        let producer =
            unsafe { Producer::from_header(header, file_size) }.expect("Failed to create producer");
        let producer2 = unsafe { Producer::from_header(header, file_size) }
            .expect("Failed to create producer2");
        let consumer = unsafe { Consumer::<Item>::from_header(header, file_size) }
            .expect("Failed to create consumer");
        let consumer2 = unsafe { Consumer::from_header(header, file_size) }
            .expect("Failed to create consumer2");
        let capacity = buffer_size_in_items;

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
}
