use crate::error::Error;
use std::{fs::File, os::fd::AsRawFd, ptr::NonNull};

/// Maps a file into memory.
pub(crate) fn map_file(file: &File, size: usize) -> Result<NonNull<u8>, Error> {
    let addr = unsafe {
        libc::mmap(
            core::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    Ok(NonNull::new(addr.cast()).expect("already checked for null"))
}
