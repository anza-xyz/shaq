use core::sync::atomic::AtomicUsize;

// NB: To simplify casting we only support 64bit or wider systems.
const _: () = assert!(size_of::<usize>() >= size_of::<u64>());

pub mod broadcast;
pub mod error;
pub mod mpmc;
mod shmem;
pub mod spsc;

pub(crate) const VERSION_MAJOR: u16 = 2;
pub(crate) const VERSION_PATCH: u16 = 0;
pub(crate) const VERSION: u32 = (VERSION_MAJOR as u32) << 16 | VERSION_PATCH as u32;

pub(crate) const fn normalized_capacity(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity.next_power_of_two()
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
