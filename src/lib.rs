use core::sync::atomic::AtomicUsize;

pub mod error;
pub mod mpmc;
mod shmem;
pub mod spsc;

pub(crate) const VERSION: u8 = 1;

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
