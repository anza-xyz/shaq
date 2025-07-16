use core::{ptr::NonNull, sync::atomic::AtomicUsize};
use std::{path::Path, sync::atomic::Ordering};

use crate::{
    error::Error,
    shmem::{create_and_map_file, open_and_map_file},
};

pub mod error;
mod shmem;

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

#[repr(C)]
struct SharedQueueHeader {
    write: CacheAlignedAtomicSize,
    read: CacheAlignedAtomicSize,
    buffer_size: usize,
}

pub const HEADER_SIZE: usize = core::mem::size_of::<SharedQueueHeader>();

impl SharedQueueHeader {
    fn create<T: Sized>(path: impl AsRef<Path>, size: usize) -> Result<NonNull<Self>, Error> {
        let buffer_size_in_items = Self::calculate_buffer_size_in_items::<T>(size)?;
        let header = create_and_map_file(path, size)?.cast::<Self>();
        Self::initialize::<T>(header, buffer_size_in_items);
        Ok(header)
    }

    const fn calculate_buffer_size_in_items<T: Sized>(file_size: usize) -> Result<usize, Error> {
        // size of buffer must be a power of two.
        // buffer must also align with T.
        let alignment = core::mem::align_of::<T>();

        let buffer_offset = core::mem::size_of::<Self>();
        // Round up to the next alignment of T - `alignment` must be a power of two.
        let buffer_offset = (buffer_offset + alignment - 1) & !(alignment - 1);

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

    fn initialize<T: Sized>(mut header: NonNull<Self>, buffer_size_in_items: usize) {
        let header = unsafe { header.as_mut() };
        header.write.store(0, Ordering::Release);
        header.read.store(0, Ordering::Release);
        header.buffer_size = buffer_size_in_items;
    }

    fn join<T: Sized>(path: impl AsRef<Path>) -> Result<NonNull<Self>, Error> {
        let (header, file_size) = open_and_map_file(path)?;
        let header = header.cast::<Self>();
        {
            let header = unsafe { header.as_ref() };
            if header.buffer_size == 0
                || !header.buffer_size.is_power_of_two()
                || header.buffer_size * core::mem::size_of::<T>() + core::mem::size_of::<Self>()
                    > file_size
            {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok(header)
    }
}

struct SharedQueue<T: Sized> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,

    buffer_mask: usize,
    cached_write: usize,
    cached_read: usize,
}

impl<T: Sized> SharedQueue<T> {
    fn from_header(header: NonNull<SharedQueueHeader>) -> Result<Self, Error> {
        let size = unsafe { header.as_ref().buffer_size };
        debug_assert!(size.is_power_of_two() && size > 0, "Invalid buffer size");

        let buffer = Self::buffer_from_header(header);

        let mut queue = Self {
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

    fn buffer_from_header(header: NonNull<SharedQueueHeader>) -> NonNull<T> {
        let after_header = unsafe { header.add(1) };
        let byte_ptr = after_header.cast::<u8>();
        let offset_to_align = byte_ptr.align_offset(core::mem::align_of::<T>());
        let aligned_ptr = unsafe { byte_ptr.byte_add(offset_to_align) };
        aligned_ptr.cast()
    }

    fn mask(&self, index: usize) -> usize {
        index & self.buffer_mask
    }

    #[inline]
    fn header(&self) -> &SharedQueueHeader {
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

pub struct Producer<T: Sized> {
    queue: SharedQueue<T>,
}

unsafe impl<T> Send for Producer<T> {}
unsafe impl<T> Sync for Producer<T> {}

impl<T: Sized> Producer<T> {
    pub fn create(path: impl AsRef<Path>, size: usize) -> Result<Self, Error> {
        let header = SharedQueueHeader::create::<T>(path, size)?;
        Self::from_header(header)
    }

    pub fn join(path: impl AsRef<Path>) -> Result<Self, Error> {
        let header = SharedQueueHeader::join::<T>(path)?;
        Self::from_header(header)
    }

    fn from_header(header: NonNull<SharedQueueHeader>) -> Result<Self, Error> {
        Ok(Self {
            queue: SharedQueue::from_header(header)?,
        })
    }

    /// Reserves a position, and increments the cached write position.
    /// Returns `None` if the queue is full.
    pub fn reserve(&mut self) -> Option<NonNull<T>> {
        // If write is >= read + buffer_size, the queue is written one iteration
        // ahead of the consumer, and we cannot reserve more space.
        if self.queue.cached_write.wrapping_sub(self.queue.cached_read)
            >= self.queue.header().buffer_size
        {
            return None;
        }

        let reserved_index = self.queue.mask(self.queue.cached_write);
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

    /// Synchronize the producer's cached read position with the queue's read position.
    pub fn sync(&mut self) {
        self.queue.load_read();
    }
}

pub struct Consumer<T: Sized> {
    queue: SharedQueue<T>,
}

unsafe impl<T> Send for Consumer<T> {}
unsafe impl<T> Sync for Consumer<T> {}

impl<T: Sized> Consumer<T> {
    pub fn create(path: impl AsRef<Path>, size: usize) -> Result<Self, Error> {
        let header = SharedQueueHeader::create::<T>(path, size)?;
        Self::from_header(header)
    }

    pub fn join(path: impl AsRef<Path>) -> Result<Self, Error> {
        let header = SharedQueueHeader::join::<T>(path)?;
        Self::from_header(header)
    }

    fn from_header(header: NonNull<SharedQueueHeader>) -> Result<Self, Error> {
        Ok(Self {
            queue: SharedQueue::from_header(header)?,
        })
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    pub fn try_read(&mut self) -> Option<NonNull<T>> {
        if self.queue.cached_read == self.queue.cached_write {
            return None; // Queue is empty
        }

        let read_index = self.queue.mask(self.queue.cached_read);
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
        SharedQueueHeader::initialize::<T>(header, buffer_size_in_items);

        (
            Producer::from_header(header).expect("Failed to create producer"),
            Consumer::from_header(header).expect("Failed to create consumer"),
        )
    }

    #[test]
    fn test_producer_consumer() {
        type Item = AtomicU64;
        const ITEM_SIZE: usize = core::mem::size_of::<Item>();
        const BUFFER_SIZE: usize = HEADER_SIZE + 1024 * ITEM_SIZE;
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let (mut producer, mut consumer) = create_test_queue::<Item>(&mut buffer);

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
        }
    }
}
