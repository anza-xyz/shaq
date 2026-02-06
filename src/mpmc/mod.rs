//! Vyukov bounded MPMC queue

use crate::{error::Error, shmem::map_file, CacheAlignedAtomicSize, VERSION};
use core::{
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
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
    ///
    /// # Safety
    /// - The provided file must be uniquely joined as a Producer.
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

    /// Writes item into the queue or returns it if there is not enough space.
    pub fn try_write(&self, item: T) -> Result<(), T> {
        self.queue.push(item)
    }

    /// Reserves a slot for writing.
    /// The slot is committed when the guard is dropped.
    pub fn reserve(&self) -> Option<WriteGuard<'_, T>> {
        self.queue
            .reserve_write()
            .map(|(cell, position)| WriteGuard {
                cell,
                position,
                _marker: PhantomData,
            })
    }

    /// Reserves up to `max` slots for writing.
    /// The slots are committed when the batch is dropped.
    pub fn reserve_batch(&self, max: usize) -> Option<WriteBatch<'_, T>> {
        self.queue
            .reserve_write_batch(max)
            .map(|(start, count)| WriteBatch {
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
    ///
    /// # Safety
    /// - The provided file must be uniquely joined as a Consumer.
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

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    pub fn try_read(&self) -> Option<T> {
        self.queue.try_pop()
    }

    /// Attempts to reserve a value from the queue, returning a guard.
    /// The slot is released back to producers when the guard is dropped.
    pub fn try_read_ptr(&self) -> Option<ReadGuard<'_, T>> {
        self.queue
            .reserve_read()
            .map(|(cell, position, buffer_mask)| ReadGuard {
                cell,
                position,
                buffer_mask,
                _marker: PhantomData,
            })
    }

    /// Attempts to reserve up to `max` values from the queue.
    /// The slots are released back to producers when the batch is dropped.
    pub fn try_read_batch(&self, max: usize) -> Option<ReadBatch<'_, T>> {
        self.queue
            .reserve_read_batch(max)
            .map(|(start, count)| ReadBatch {
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
    buffer_offset + capacity * core::mem::size_of::<Cell<T>>()
}

#[repr(C)]
struct SharedQueue<T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<Cell<T>>,
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
    pub fn push(&self, value: T) -> Result<(), T> {
        let position = match self.reserve_position() {
            Some(position) => position,
            None => return Err(value),
        };
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds. Same cell is not accessed concurrently.
        let cell = unsafe { self.buffer.add(cell_index).as_mut() };
        cell.value.write(value);
        cell.sequence.store(position + 1, Ordering::Release);
        Ok(())
    }

    pub fn try_pop(&self) -> Option<T> {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut position = header.read_position.load(Ordering::Relaxed);
        loop {
            let cell = unsafe { self.buffer.add(position & self.buffer_mask).as_ref() };
            let sequence = cell.sequence.load(Ordering::Acquire);
            match sequence.cmp(&position.wrapping_add(1)) {
                core::cmp::Ordering::Less => return None,
                core::cmp::Ordering::Equal => {
                    if header
                        .read_position
                        .compare_exchange_weak(
                            position,
                            position.wrapping_add(1),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        let value = unsafe { cell.value.as_ptr().read() };
                        cell.sequence.store(
                            position.wrapping_add(self.buffer_mask + 1),
                            Ordering::Release,
                        );
                        return Some(value);
                    }
                }
                core::cmp::Ordering::Greater => {
                    position = header.read_position.load(Ordering::Relaxed);
                    continue;
                }
            }
        }
    }

    // Reserve a cell for writing, returning its position if successful.
    fn reserve_position(&self) -> Option<usize> {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut position = header.write_position.load(Ordering::Relaxed);
        loop {
            // SAFETY: Mask ensures index is in bounds
            let cell = unsafe { self.buffer.add(position & self.buffer_mask).as_ref() };
            let sequence = cell.sequence.load(Ordering::Relaxed);
            match sequence.cmp(&position) {
                core::cmp::Ordering::Less => return None,
                core::cmp::Ordering::Equal => {
                    if header
                        .write_position
                        .compare_exchange_weak(
                            position,
                            position.wrapping_add(1),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        return Some(position);
                    }
                    position = header.write_position.load(Ordering::Relaxed);
                }
                core::cmp::Ordering::Greater => {
                    position = header.write_position.load(Ordering::Relaxed);
                }
            }
        }
    }

    fn reserve_write(&self) -> Option<(NonNull<Cell<T>>, usize)> {
        let position = self.reserve_position()?;
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, position))
    }

    fn reserve_read(&self) -> Option<(NonNull<Cell<T>>, usize, usize)> {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };

        let mut position = header.read_position.load(Ordering::Relaxed);
        loop {
            let cell = unsafe { self.buffer.add(position & self.buffer_mask) };
            let sequence = unsafe { cell.as_ref().sequence.load(Ordering::Acquire) };
            match sequence.cmp(&position.wrapping_add(1)) {
                core::cmp::Ordering::Less => return None,
                core::cmp::Ordering::Equal => {
                    if header
                        .read_position
                        .compare_exchange_weak(
                            position,
                            position.wrapping_add(1),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        return Some((cell, position, self.buffer_mask));
                    }
                }
                core::cmp::Ordering::Greater => {
                    position = header.read_position.load(Ordering::Relaxed);
                    continue;
                }
            }
        }
    }

    fn reserve_write_batch(&self, max: usize) -> Option<(usize, usize)> {
        if max == 0 {
            return None;
        }

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut position = header.write_position.load(Ordering::Relaxed);

        'outer: loop {
            let mut count = 0usize;
            let mut saw_full = false;
            while count < max {
                let pos = position.wrapping_add(count);
                let cell = unsafe { self.buffer.add(pos & self.buffer_mask).as_ref() };
                let sequence = cell.sequence.load(Ordering::Relaxed);
                match sequence.cmp(&pos) {
                    core::cmp::Ordering::Less => {
                        saw_full = true;
                        break;
                    }
                    core::cmp::Ordering::Equal => {
                        count += 1;
                    }
                    core::cmp::Ordering::Greater => {
                        position = header.write_position.load(Ordering::Relaxed);
                        continue 'outer;
                    }
                }
            }

            if count == 0 {
                if saw_full {
                    return None;
                }
                continue;
            }

            let new_position = position.wrapping_add(count);
            if header
                .write_position
                .compare_exchange_weak(position, new_position, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some((position, count));
            }

            position = header.write_position.load(Ordering::Relaxed);
        }
    }

    fn reserve_read_batch(&self, max: usize) -> Option<(usize, usize)> {
        if max == 0 {
            return None;
        }

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut position = header.read_position.load(Ordering::Relaxed);

        'outer: loop {
            let mut count = 0usize;
            let mut saw_empty = false;
            while count < max {
                let pos = position.wrapping_add(count);
                let cell = unsafe { self.buffer.add(pos & self.buffer_mask).as_ref() };
                let expected = pos.wrapping_add(1);
                let sequence = cell.sequence.load(Ordering::Acquire);
                match sequence.cmp(&expected) {
                    core::cmp::Ordering::Less => {
                        saw_empty = true;
                        break;
                    }
                    core::cmp::Ordering::Equal => {
                        count += 1;
                    }
                    core::cmp::Ordering::Greater => {
                        position = header.read_position.load(Ordering::Relaxed);
                        continue 'outer;
                    }
                }
            }

            if count == 0 {
                if saw_empty {
                    return None;
                }
                continue;
            }

            let new_position = position.wrapping_add(count);
            if header
                .read_position
                .compare_exchange_weak(position, new_position, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some((position, count));
            }

            position = header.read_position.load(Ordering::Relaxed);
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
    ///   header and padding bytes to align the trailing buffer of `Cell<T>`.
    unsafe fn buffer_from_header(header: NonNull<SharedQueueHeader>) -> NonNull<Cell<T>> {
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
    read_position: CacheAlignedAtomicSize,
    write_position: CacheAlignedAtomicSize,
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
        unsafe { Self::initialize::<T>(header, buffer_size_in_items) };
        Ok(header)
    }

    const fn buffer_offset<T: Sized>() -> usize {
        core::mem::size_of::<Self>().next_multiple_of(core::mem::align_of::<Cell<T>>())
    }

    const fn calculate_buffer_size_in_items<T: Sized>(file_size: usize) -> Result<usize, Error> {
        let buffer_offset = Self::buffer_offset::<T>();
        if file_size < buffer_offset {
            return Err(Error::InvalidBufferSize);
        }

        // The buffer size (in units of T) must be a power of two.
        let buffer_size_in_bytes = file_size - buffer_offset;
        let mut buffer_size_in_items = buffer_size_in_bytes / core::mem::size_of::<Cell<T>>();
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
    unsafe fn initialize<T: Sized>(mut header_ptr: NonNull<Self>, buffer_size_in_items: usize) {
        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header_ptr.as_mut() };
        header.read_position.store(0, Ordering::Release);
        header.write_position.store(0, Ordering::Release);
        header.buffer_mask = buffer_size_in_items - 1;
        header.version.store(VERSION, Ordering::SeqCst);

        let buffer = unsafe {
            header_ptr
                .byte_add(Self::buffer_offset::<T>())
                .cast::<Cell<T>>()
        };
        for index in 0..buffer_size_in_items {
            let cell = unsafe { buffer.add(index).as_mut() };
            cell.sequence.store(index, Ordering::Release);
        }
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
}

#[repr(C)]
struct Cell<T> {
    sequence: AtomicUsize,
    value: MaybeUninit<T>,
}

pub struct WriteGuard<'a, T> {
    cell: NonNull<Cell<T>>,
    position: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> WriteGuard<'a, T> {
    pub fn as_mut_ptr(&mut self) -> *mut T {
        // SAFETY: The cell was reserved for writing.
        unsafe { self.cell.as_mut().value.as_mut_ptr() }
    }

    pub fn write(mut self, value: T) {
        // SAFETY: The cell was reserved for writing.
        unsafe { self.cell.as_mut().value.write(value) };
    }
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: The cell was reserved for writing, so publishing is safe.
        unsafe {
            self.cell
                .as_mut()
                .sequence
                .store(self.position + 1, Ordering::Release);
        }
    }
}

pub struct ReadGuard<'a, T> {
    cell: NonNull<Cell<T>>,
    position: usize,
    buffer_mask: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> ReadGuard<'a, T> {
    pub fn as_ptr(&self) -> *const T {
        // SAFETY: The cell was reserved for reading.
        unsafe { self.cell.as_ref().value.as_ptr() }
    }

    pub fn read(self) -> T {
        // SAFETY: The cell was reserved for reading and holds an initialized value.
        unsafe { self.cell.as_ref().value.as_ptr().read() }
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: The cell was reserved for reading, so publishing is safe.
        unsafe {
            self.cell.as_mut().sequence.store(
                self.position.wrapping_add(self.buffer_mask + 1),
                Ordering::Release,
            );
        }
    }
}

pub struct WriteBatch<'a, T> {
    buffer: NonNull<Cell<T>>,
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
        unsafe {
            &mut *self
                .buffer
                .add(position & self.buffer_mask)
                .as_mut()
                .value
                .as_mut_ptr()
        }
    }

    pub fn as_mut_ptr(&mut self, index: usize) -> *mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe {
            let cell = self.buffer.add(position & self.buffer_mask).as_mut();
            cell.value.as_mut_ptr()
        }
    }
}

impl<'a, T> Drop for WriteBatch<'a, T> {
    fn drop(&mut self) {
        for index in 0..self.count {
            let position = self.start.wrapping_add(index);
            // SAFETY: Each position was reserved for writing, so publishing is safe.
            unsafe {
                self.buffer
                    .add(position & self.buffer_mask)
                    .as_mut()
                    .sequence
                    .store(position.wrapping_add(1), Ordering::Release);
            }
        }
    }
}

pub struct ReadBatch<'a, T> {
    buffer: NonNull<Cell<T>>,
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
        unsafe {
            let cell = self.buffer.add(position & self.buffer_mask).as_ref();
            cell.value.as_ptr()
        }
    }
}

impl<'a, T> Drop for ReadBatch<'a, T> {
    fn drop(&mut self) {
        for index in 0..self.count {
            let position = self.start.wrapping_add(index);
            // SAFETY: Each position was reserved for reading, so publishing is safe.
            unsafe {
                self.buffer
                    .add(position & self.buffer_mask)
                    .as_mut()
                    .sequence
                    .store(
                        position.wrapping_add(self.buffer_mask + 1),
                        Ordering::Release,
                    );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ptr::NonNull;
    use std::alloc::{alloc_zeroed, dealloc, Layout};

    struct AlignedBuffer {
        ptr: NonNull<u8>,
        len: usize,
    }

    impl AlignedBuffer {
        fn new(len: usize) -> Self {
            let layout = Layout::from_size_align(len, 64).expect("invalid layout");
            // SAFETY: Layout has non-zero size and valid alignment.
            let ptr = unsafe { alloc_zeroed(layout) };
            let ptr = NonNull::new(ptr).expect("alloc failed");
            Self { ptr, len }
        }

        fn as_mut_slice(&mut self) -> &mut [u8] {
            // SAFETY: The allocation is valid for `len` bytes.
            unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
        }

        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.ptr.as_ptr()
        }

        fn len(&self) -> usize {
            self.len
        }
    }

    impl Drop for AlignedBuffer {
        fn drop(&mut self) {
            // SAFETY: The allocation matches this layout and is still live.
            unsafe {
                dealloc(
                    self.ptr.as_ptr(),
                    Layout::from_size_align(self.len, 64).expect("invalid layout"),
                );
            }
        }
    }

    fn create_test_queue<T: Sized>(buffer: &mut [u8]) -> (Producer<T>, Consumer<T>) {
        let file_size = buffer.len();
        let buffer_size_in_items =
            SharedQueueHeader::calculate_buffer_size_in_items::<T>(file_size)
                .expect("Invalid buffer size");
        let header = NonNull::new(buffer.as_mut_ptr().cast()).expect("Failed to create header");
        unsafe { SharedQueueHeader::initialize::<T>(header, buffer_size_in_items) };

        (
            unsafe { Producer::from_header(header, file_size) }.expect("Failed to create producer"),
            unsafe { Consumer::from_header(header, file_size) }.expect("Failed to create consumer"),
        )
    }

    #[test]
    fn test_producer_consumer() {
        type Item = u64;
        const BUFFER_CAPACITY: usize = 1024;
        const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);
        let mut buffer = AlignedBuffer::new(BUFFER_SIZE);
        let (producer, consumer) = create_test_queue::<Item>(buffer.as_mut_slice());
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
        type Item = u64;
        const BUFFER_CAPACITY: usize = 8;
        const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);
        let mut buffer = AlignedBuffer::new(BUFFER_SIZE);
        let (producer, consumer) = create_test_queue::<Item>(buffer.as_mut_slice());

        let mut guard = producer.reserve().expect("reserve failed");
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
        type Item = u64;
        const BUFFER_CAPACITY: usize = 16;
        const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);
        let mut buffer = AlignedBuffer::new(BUFFER_SIZE);
        let (producer, consumer) = create_test_queue::<Item>(buffer.as_mut_slice());

        let mut batch = producer.reserve_batch(4).expect("reserve_batch failed");
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
    fn test_multiple_producers_consumers() {
        type Item = u64;
        const BUFFER_CAPACITY: usize = 64;
        const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);
        let mut buffer = AlignedBuffer::new(BUFFER_SIZE);
        let file_size = buffer.len();
        let buffer_size_in_items =
            SharedQueueHeader::calculate_buffer_size_in_items::<Item>(file_size)
                .expect("Invalid buffer size");
        let header = NonNull::new(buffer.as_mut_ptr().cast()).expect("Failed to create header");
        unsafe { SharedQueueHeader::initialize::<Item>(header, buffer_size_in_items) };

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
