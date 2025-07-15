use core::{ptr::NonNull, sync::atomic::AtomicUsize};
use std::sync::atomic::Ordering;

pub mod error;
pub mod shmem;

/// `AtomicUsize` with 64-byte alignment for better performance.
#[derive(Default)]
#[repr(C, align(64))]
pub struct CacheAlignedAtomicSize {
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
}

pub struct SharedQueue<T: Sized> {
    header_ptr: NonNull<SharedQueueHeader>,
    buffer_ptr: NonNull<T>,
    buffer_size: usize,
    buffer_mask: usize,
}

impl<T: Sized> SharedQueue<T> {
    fn mask(&self, index: usize) -> usize {
        index & self.buffer_mask
    }

    #[inline]
    fn header(&self) -> &SharedQueueHeader {
        unsafe { self.header_ptr.as_ref() }
    }
}

struct Producer<T: Sized> {
    queue: SharedQueue<T>,
    cached_write: usize,
    cached_read: usize,
}

impl<T: Sized> Producer<T> {
    /// Synchronizes cached values with those in shared memory.
    pub fn initial_sync(&mut self) {
        self.cached_write = self.queue.header().write.load(Ordering::Acquire);
        self.sync();
    }

    /// Reserves a position, and increments the cached write position.
    /// Returns `None` if the queue is full.
    pub fn reserve(&mut self) -> Option<NonNull<T>> {
        if self.cached_write.wrapping_sub(self.cached_read) >= self.queue.buffer_size {
            return None; // Queue is full
        }

        let reserved_index = self.queue.mask(self.cached_read);
        let reserved_ptr = unsafe { self.queue.buffer_ptr.add(reserved_index) };
        self.cached_write = self.cached_write.wrapping_add(1);

        Some(reserved_ptr)
    }

    /// Commits the reserved position, making it visible to the consumer.
    pub fn commit(&self) {
        self.queue
            .header()
            .write
            .store(self.cached_write, Ordering::Release);
    }

    /// Synchronize the producer's cached read position with the queue's read position.
    pub fn sync(&mut self) {
        self.cached_read = self.queue.header().read.load(Ordering::Acquire);
    }
}

struct Consumer<T: Sized> {
    queue: SharedQueue<T>,
    cached_read: usize,
    cached_write: usize,
}

impl<T: Sized> Consumer<T> {
    /// Synchronizes cached values with those in shared memory.
    pub fn initial_sync(&mut self) {
        self.cached_read = self.queue.header().read.load(Ordering::Acquire);
        self.sync();
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    pub fn try_read(&mut self) -> Option<NonNull<T>> {
        if self.cached_read == self.cached_write {
            return None; // Queue is empty
        }

        let read_index = self.queue.mask(self.cached_read);
        let read_ptr = unsafe { self.queue.buffer_ptr.add(read_index) };
        self.cached_read = self.cached_read.wrapping_add(1);

        Some(read_ptr)
    }

    /// Synchronizes the consumer's cached write position with the queue's write position.
    pub fn sync(&mut self) {
        self.cached_write = self.queue.header().write.load(Ordering::Acquire);
    }
}
