use core::{ptr::NonNull, sync::atomic::AtomicUsize};
use std::{fs::File, sync::atomic::Ordering};

use crate::{error::Error, shmem::map_file};

pub mod error;
mod shmem;

/// Calculates the minimum file size required for a queue with given capacity.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
pub const fn minimum_file_size<T: Sized>(capacity: usize) -> usize {
    let buffer_offset = SharedQueueHeader::buffer_offset::<T>();
    buffer_offset + capacity * core::mem::size_of::<T>()
}

/// Producer side of the SPSC shared queue.
pub struct Producer<T: Sized> {
    queue: SharedQueue<T>,
}

impl<T: Sized> Producer<T> {
    /// Creates a new producer for the shared queue in the provided file with
    /// the given size.
    pub fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        let header = SharedQueueHeader::create::<T>(file, file_size)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(header, file_size) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    ///
    /// # SAFETY: The provided file must be uniquely accessed as a Producer.
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

    /// Return the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Return the current length of the queue.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Reserves a position, and increments the cached write position.
    /// Returns `None` if the queue is full.
    /// Returns a pointer to the reserved position.
    ///
    /// All reserved positions should be written and pointers discarded before
    /// calling `commit`.
    pub fn reserve(&mut self) -> Option<NonNull<T>> {
        // If write is >= read + buffer_size, the queue is written one iteration
        // ahead of the consumer, and we cannot reserve more space.
        if self.queue.cached_write.wrapping_sub(self.queue.cached_read)
            >= self.queue.header().buffer_size
        {
            return None;
        }

        let reserved_index = self.queue.mask(self.queue.cached_write);
        // SAFETY: The reserved index is guaranteed to be within bounds given the mask.
        let reserved_ptr = unsafe { self.queue.buffer.add(reserved_index) };
        self.queue.cached_write = self.queue.cached_write.wrapping_add(1);

        Some(reserved_ptr)
    }

    /// Commits the reserved position, making it visible to the consumer.
    pub fn commit(&self) {
        self.queue
            .header()
            .write
            .store(self.queue.cached_write, Ordering::Release);
    }

    /// Synchronize the producer's cached read position with the queue's read
    /// position.
    pub fn sync(&mut self) {
        self.queue.load_read();
    }
}

/// Consumer side of the SPSC shared queue.
pub struct Consumer<T: Sized> {
    queue: SharedQueue<T>,
}

impl<T: Sized> Consumer<T> {
    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    pub fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        let header = SharedQueueHeader::create::<T>(file, file_size)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(header, file_size) }
    }

    /// Joins an existing consumer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - The provided file must be uniquely accessed as a Consumer.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
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

    /// Return the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Return the current length of the queue.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    /// Returns a pointer to the value if available.
    ///
    /// All read items should be processed and pointers discarded before
    /// calling `finalize`.
    pub fn try_read(&mut self) -> Option<NonNull<T>> {
        if self.queue.cached_read == self.queue.cached_write {
            return None; // Queue is empty
        }

        let read_index = self.queue.mask(self.queue.cached_read);
        // SAFETY: read_index is guaranteed to be within bounds given the mask.
        let read_ptr = unsafe { self.queue.buffer.add(read_index) };
        self.queue.cached_read = self.queue.cached_read.wrapping_add(1);

        Some(read_ptr)
    }

    /// Publishes the read position, making it visible to the producer.
    /// All previously read items MUST be processed before this is called.
    pub fn finalize(&self) {
        self.queue
            .header()
            .read
            .store(self.queue.cached_read, Ordering::Release);
    }

    /// Synchronizes the consumer's cached write position with the queue's write position.
    pub fn sync(&mut self) {
        self.queue.load_write();
    }
}

struct SharedQueue<T: Sized> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,

    file_size: usize,
    buffer_mask: usize,
    cached_write: usize,
    cached_read: usize,
}

impl<T> Drop for SharedQueue<T> {
    fn drop(&mut self) {
        // Tests do not mmap so skip unmapping in tests.
        #[cfg(test)]
        {
            return;
        }

        #[allow(unreachable_code)]
        // SAFETY: buffer is mmapped and of size `file_size`.
        unsafe {
            libc::munmap(self.buffer.as_ptr().cast(), self.file_size);
        }
    }
}

impl<T: Sized> SharedQueue<T> {
    /// Creates a new shared queue from a header pointer and file size.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation at `header` must be of size `file_size`.
    unsafe fn from_header(
        header: NonNull<SharedQueueHeader>,
        file_size: usize,
    ) -> Result<Self, Error> {
        // SAFETY: `header` is non-null and aligned properly.
        let size = unsafe { header.as_ref().buffer_size };

        if !size.is_power_of_two()
            || size == 0
            || SharedQueueHeader::calculate_buffer_size_in_items::<T>(file_size)? != size
        {
            return Err(Error::InvalidBufferSize);
        }

        // SAFETY: `header` is non-null and aligned properly with allocation
        //         of size file_size.
        let buffer = unsafe { Self::buffer_from_header(header) };

        let mut queue = Self {
            file_size,
            header,
            buffer,
            buffer_mask: size - 1,
            cached_write: 0,
            cached_read: 0,
        };

        queue.load_write();
        queue.load_read();

        Ok(queue)
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

    fn capacity(&self) -> usize {
        self.buffer_mask + 1
    }

    fn len(&self) -> usize {
        self.cached_write.wrapping_sub(self.cached_read)
    }

    fn is_empty(&self) -> bool {
        self.cached_write == self.cached_read
    }

    fn mask(&self, index: usize) -> usize {
        index & self.buffer_mask
    }

    #[inline]
    fn header(&self) -> &SharedQueueHeader {
        // SAFETY: See safety on `from_header`. `header` is non-null and aligned.
        unsafe { self.header.as_ref() }
    }

    #[inline]
    fn load_write(&mut self) {
        self.cached_write = self.header().write.load(Ordering::Acquire);
    }

    #[inline]
    fn load_read(&mut self) {
        self.cached_read = self.header().read.load(Ordering::Acquire);
    }
}

/// Header in shared memory for the queue.
#[repr(C)]
struct SharedQueueHeader {
    write: CacheAlignedAtomicSize,
    read: CacheAlignedAtomicSize,
    buffer_size: usize,
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
        (core::mem::size_of::<Self>() + core::mem::align_of::<T>() - 1)
            & !(core::mem::align_of::<T>() - 1)
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
    unsafe fn initialize(mut header: NonNull<Self>, buffer_size_in_items: usize) {
        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header.as_mut() };
        header.write.store(0, Ordering::Release);
        header.read.store(0, Ordering::Release);
        header.buffer_size = buffer_size_in_items;
    }

    fn join<T: Sized>(file: &File) -> Result<(NonNull<Self>, usize), Error> {
        let file_size = file.metadata()?.len() as usize;
        let header = map_file(file, file_size)?;
        let header = header.cast::<Self>();
        {
            // SAFETY: The header is non-null and aligned properly.
            //         Alignment is guaranteed because `open_and_map_file` will return
            //         a pointer only if mapping was successful. mmap ensures that the
            //         memory is aligned to the page size, which is sufficient for the
            //         alignment of `SharedQueueHeader`.
            let header = unsafe { header.as_ref() };
            if header.buffer_size != Self::calculate_buffer_size_in_items::<T>(file_size)? {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok((header, file_size))
    }
}

/// `AtomicUsize` with 64-byte alignment for better performance.
#[derive(Default)]
#[repr(C, align(64))]
struct CacheAlignedAtomicSize {
    inner: AtomicUsize,
}

impl core::ops::Deref for CacheAlignedAtomicSize {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

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
        type Item = AtomicU64;
        const BUFFER_CAPACITY: usize = 1024;
        const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let (mut producer, mut consumer) = create_test_queue::<Item>(&mut buffer);

        assert_eq!(producer.capacity(), BUFFER_CAPACITY);
        assert_eq!(consumer.capacity(), BUFFER_CAPACITY);

        unsafe {
            producer
                .reserve()
                .expect("Failed to reserve")
                .as_ref()
                .store(42, Ordering::Release);
            assert!(consumer.try_read().is_none()); // not committed yet
            producer.commit();
            assert!(consumer.try_read().is_none()); // consumer has not synced yet
            consumer.sync();
            let item = consumer.try_read().expect("Failed to read item");
            assert_eq!(item.as_ref().load(Ordering::Acquire), 42);
            assert!(consumer.try_read().is_none()); // no more items to read
            consumer.finalize();
            producer.sync();

            // Ensure we can push up to the capacity.
            for _ in 0..BUFFER_CAPACITY {
                let spot = producer.reserve().expect("Failed to reserve");
                spot.as_ref().store(1, Ordering::Release);
            }
            assert!(producer.reserve().is_none()); // buffer is full, we cannot reserve more
            producer.commit();
            consumer.sync();
            for _ in 0..BUFFER_CAPACITY {
                let item = consumer.try_read().expect("Failed to read item");
                assert_eq!(item.as_ref().load(Ordering::Acquire), 1);
            }
            assert!(consumer.try_read().is_none()); // no more items to read
            consumer.finalize();
            producer.sync();

            // Ensure we can reserve again after finalizing/sync.
            let spot = producer
                .reserve()
                .expect("Failed to reserve after finalize");
            spot.as_ref().store(2, Ordering::Release);
            producer.commit();
            consumer.sync();
            let item = consumer
                .try_read()
                .expect("Failed to read item after finalize");
            assert_eq!(item.as_ref().load(Ordering::Acquire), 2);
            consumer.finalize();
        }
    }
}
