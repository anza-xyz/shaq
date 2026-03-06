use core::sync::atomic::AtomicUsize;

pub mod error;
pub mod mpmc;
mod shmem;
pub mod spsc;

pub(crate) const VERSION_MAJOR: u8 = 2;
pub(crate) const VERSION_PATCH: u8 = 0;
pub(crate) const VERSION: u16 = (VERSION_MAJOR as u16) << 8 | VERSION_PATCH as u16;

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
