use crate::error::Error;
use std::{fs::File, os::fd::AsRawFd, ptr::NonNull};

/// Maps a file into memory.
pub(crate) fn map_file(file: &File, size: usize) -> Result<NonNull<u8>, Error> {
    #[cfg(target_os = "linux")]
    {
        // On linux first attempt to map with hugepages. Fall back to regular on failure.
        let r = map_file_with_flags(file, size, libc::MAP_SHARED | libc::MAP_HUGETLB);
        if r.is_ok() {
            return r;
        }
    }

    map_file_with_flags(file, size, libc::MAP_SHARED)
}

fn map_file_with_flags(file: &File, size: usize, flags: libc::c_int) -> Result<NonNull<u8>, Error> {
    let addr = unsafe {
        libc::mmap(
            core::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            file.as_raw_fd(),
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    Ok(NonNull::new(addr.cast()).expect("already checked for null"))
}
