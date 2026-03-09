use crate::{error::Error, shmem::MappedRegion, CacheAlignedAtomicSize, VERSION};
use core::ptr::NonNull;
use std::{
    fs::File,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

const MAGIC: u64 = 0x7368_6171_7370_7363; // b"shaqspsc"

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
    /// - The provided file must be uniquely joined as a Producer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Consumer`] that is joined with the
    ///   same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    ///
    /// # Safety
    /// - The caller must ensure this is the unique Consumer for this queue.
    pub unsafe fn join_as_consumer(&self) -> Result<Consumer<T>, Error> {
        // SAFETY: caller guarantees uniqueness of the consumer role.
        unsafe { Consumer::from_header(Arc::clone(&self.queue.region), self.queue.header) }
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

    /// Writes item into the queue or returns it if there is not enough space.
    pub fn try_write(&mut self, item: T) -> Result<(), T> {
        // SAFETY: pointer is written below if successfully reserved.
        match unsafe { self.reserve() } {
            Some(p) => {
                // SAFETY: `reserve` returns a properly aligned ptr with enough
                //         space to write T.
                unsafe { p.write(item) };
                Ok(())
            }
            None => Err(item),
        }
    }

    /// Reserves a position, and increments the cached write position.
    /// Returns `None` if the queue is full.
    /// Returns a pointer to the reserved position.
    ///
    /// # Safety
    /// All reserved positions must be fully initialized before calling `commit`.
    /// Pointers should be dropped before calling `commit`.
    pub unsafe fn reserve(&mut self) -> Option<NonNull<T>> {
        // If write is > read + buffer_mask, the queue is written one iteration
        // ahead of the consumer, and we cannot reserve more space.
        if self.queue.cached_write.wrapping_sub(self.queue.cached_read) > self.queue.buffer_mask {
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

unsafe impl<T> Send for Producer<T> where T: Sized {}

/// Consumer side of the SPSC shared queue.
pub struct Consumer<T: Sized> {
    queue: SharedQueue<T>,
}

impl<T: Sized> Consumer<T> {
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
    /// - The provided file must be uniquely joined as a Consumer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Producer`] that is joined with the
    ///   same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Producer that shares the same memory mapping.
    ///
    /// # Safety
    /// - The caller must ensure this is the unique Producer for this queue.
    pub unsafe fn join_as_producer(&self) -> Result<Producer<T>, Error> {
        // SAFETY: caller guarantees uniqueness of the producer role.
        unsafe { Producer::from_header(Arc::clone(&self.queue.region), self.queue.header) }
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
    /// Returns a reference to the value if available.
    pub fn try_read(&mut self) -> Option<&T> {
        // SAFETY: `try_read_ptr` returns a pointer to properly aligned
        //         location for `T`.
        //         IF producer properly wrote items, or T is POD, it is
        //         safe to convert to reference here.
        self.try_read_ptr().map(|p| unsafe { p.as_ref() })
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    /// Returns a pointer to the value if available.
    ///
    /// All read items should be processed and pointers discarded before
    /// calling `finalize`.
    pub fn try_read_ptr(&mut self) -> Option<NonNull<T>> {
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
    pub fn finalize(&mut self) {
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

unsafe impl<T> Send for Consumer<T> where T: Sized {}

struct SharedQueue<T: Sized> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,

    buffer_mask: usize,
    cached_write: usize,
    cached_read: usize,

    // NB: Region must be declared last so it is dropped last ensuring `header` and
    // `buffer` remain valid for their entire lifetime.
    region: Arc<MappedRegion>,
}

impl<T: Sized> SharedQueue<T> {
    /// Creates a new shared queue from a header pointer and region.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - `region` must back the allocation at `header`.
    unsafe fn from_header(
        region: Arc<MappedRegion>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        // SAFETY: `header` is non-null and aligned properly.
        let size = unsafe { (header.as_ref().buffer_mask as usize).wrapping_add(1) };

        if !size.is_power_of_two()
            || SharedQueueHeader::calculate_buffer_size_in_items::<T>(region.file_size())? != size
        {
            return Err(Error::InvalidBufferSize);
        }

        // SAFETY: `header` is non-null and aligned properly with allocation
        //         of sufficient size.
        let buffer = unsafe { Self::buffer_from_header(header) };

        let mut queue = Self {
            region,
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
    // Cold cache line.
    magic: AtomicU64,
    version: u32,
    buffer_mask: u32,

    // Hot cache lines.
    write: CacheAlignedAtomicSize,
    read: CacheAlignedAtomicSize,
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
        header.buffer_mask = (buffer_size_in_items - 1) as u32;
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
            if header.buffer_mask as usize + 1
                != Self::calculate_buffer_size_in_items::<T>(file_size)?
            {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok((region, header))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::create_temp_shmem_file;
    use std::sync::atomic::AtomicU64;

    fn create_test_queue<T: Sized>(file_size: usize) -> (File, Producer<T>, Consumer<T>) {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::create(&file, file_size) }.expect("Failed to create producer");
        let consumer = unsafe { Consumer::join(&file) }.expect("Failed to join consumer");

        (file, producer, consumer)
    }

    #[test]
    fn test_producer_consumer() {
        type Item = AtomicU64;
        const BUFFER_CAPACITY: usize = 1024;
        const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);

        let (_file, mut producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

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
            assert_eq!(item.load(Ordering::Acquire), 42);
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
                assert_eq!(item.load(Ordering::Acquire), 1);
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
            assert_eq!(item.load(Ordering::Acquire), 2);
            consumer.finalize();
        }
    }

    #[test]
    fn test_join_producer_as_consumer() {
        const BUFFER_CAPACITY: usize = 64;
        const BUFFER_SIZE: usize = minimum_file_size::<u64>(BUFFER_CAPACITY);

        let file = create_temp_shmem_file().unwrap();
        let mut producer =
            unsafe { Producer::<u64>::create(&file, BUFFER_SIZE) }.expect("create failed");
        // SAFETY: this is the unique consumer for this queue.
        let mut consumer = unsafe { producer.join_as_consumer() }.expect("join failed");

        producer.try_write(42).unwrap();
        producer.commit();
        consumer.sync();
        let val = consumer.try_read().expect("read failed");
        assert_eq!(*val, 42);
        consumer.finalize();
    }

    #[test]
    fn test_join_consumer_as_producer() {
        const BUFFER_CAPACITY: usize = 64;
        const BUFFER_SIZE: usize = minimum_file_size::<u64>(BUFFER_CAPACITY);

        let file = create_temp_shmem_file().unwrap();
        let mut consumer =
            unsafe { Consumer::<u64>::create(&file, BUFFER_SIZE) }.expect("create failed");
        // SAFETY: this is the unique producer for this queue.
        let mut producer = unsafe { consumer.join_as_producer() }.expect("join failed");

        producer.try_write(99).unwrap();
        producer.commit();
        consumer.sync();
        let val = consumer.try_read().expect("read failed");
        assert_eq!(*val, 99);
        consumer.finalize();
    }

    #[test]
    fn test_drop_order_independent() {
        const BUFFER_CAPACITY: usize = 64;
        const BUFFER_SIZE: usize = minimum_file_size::<u64>(BUFFER_CAPACITY);

        let file = create_temp_shmem_file().unwrap();
        let mut producer =
            unsafe { Producer::<u64>::create(&file, BUFFER_SIZE) }.expect("create failed");
        // SAFETY: this is the unique consumer for this queue.
        let mut consumer = unsafe { producer.join_as_consumer() }.expect("join failed");

        // Write a message then drop.
        producer.try_write(7).unwrap();
        producer.commit();
        drop(producer);

        // Can still read the message from the shared consumer
        consumer.sync();
        let val = consumer.try_read().expect("read after producer drop");
        assert_eq!(*val, 7);
        consumer.finalize();
    }
}
