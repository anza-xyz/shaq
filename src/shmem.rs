use crate::error::Error;
use std::{fs::File, ptr::NonNull};

/// Maps a file into memory.
#[cfg(unix)]
pub(crate) fn map_file(file: &File, size: usize) -> Result<NonNull<u8>, Error> {
    use std::os::fd::AsRawFd;

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

/// Unmaps a previously mapped file view.
#[cfg(unix)]
pub(crate) unsafe fn unmap_file(addr: NonNull<u8>, size: usize) {
    let _ = unsafe { libc::munmap(addr.as_ptr().cast(), size) };
}

/// Maps a file into memory.
#[cfg(windows)]
pub(crate) fn map_file(file: &File, size: usize) -> Result<NonNull<u8>, Error> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Memory::{
        CreateFileMappingW, MapViewOfFile, FILE_MAP_ALL_ACCESS, PAGE_READWRITE,
    };

    let size_u64 = u64::try_from(size).map_err(|_| Error::InvalidBufferSize)?;
    let size_high = (size_u64 >> 32) as u32;
    let size_low = size_u64 as u32;

    let mapping = unsafe {
        CreateFileMappingW(
            file.as_raw_handle() as HANDLE,
            core::ptr::null(),
            PAGE_READWRITE,
            size_high,
            size_low,
            core::ptr::null(),
        )
    };

    if mapping.is_null() {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    let mmap = unsafe { MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, size) };

    unsafe {
        CloseHandle(mapping);
    }

    if mmap.Value.is_null() {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    Ok(NonNull::new(mmap.Value.cast()).expect("already checked for null"))
}

/// Unmaps a previously mapped file view.
#[cfg(windows)]
pub(crate) unsafe fn unmap_file(addr: NonNull<u8>, _size: usize) {
    use windows_sys::Win32::System::Memory::{UnmapViewOfFile, MEMORY_MAPPED_VIEW_ADDRESS};

    let _ = unsafe {
        UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
            Value: addr.cast().as_ptr(),
        })
    };
}
