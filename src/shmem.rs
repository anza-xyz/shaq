use crate::error::Error;
use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
    ptr::NonNull,
};

/// Creates a new file at the specified path, with the given size, and maps it
/// into memory.
/// Returns a pointer to the mapped memory.
pub fn create_and_map_file(path: impl AsRef<Path>, size: usize) -> Result<NonNull<u8>, Error> {
    let file = create_file(path.as_ref(), size)?;
    let mmap = map(&file, size)?;
    Ok(NonNull::new(mmap).expect("already checked for null"))
}

/// Opens an existing file at the specified path, maps it into memory, and
/// returns a pointer to the mapped memory along with the size of the file.
pub fn open_and_map_file(path: impl AsRef<Path>) -> Result<(NonNull<u8>, usize), Error> {
    let file = open_file(path.as_ref())?;
    let file_size = file.metadata()?.len() as usize;
    let mmap = map(&file, file_size)?;
    let ptr = NonNull::new(mmap).expect("already checked for null");
    Ok((ptr, file_size))
}

/// Create a new file at the specified path with the given size.
/// If the file already exists, it will return an error.
/// If the file is created successfully, it will be truncated to the specified size.
/// Returns the file handle.
fn create_file(path: impl AsRef<Path>, size: usize) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path.as_ref())?;

    file.set_len(size as u64)?;
    Ok(file)
}

/// Opens an existing file at the specified path.
/// Returns the file handle.
fn open_file(path: impl AsRef<Path>) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.as_ref())?;
    Ok(file)
}

/// Maps a file into memory.
fn map(file: &File, size: usize) -> Result<*mut u8, Error> {
    let addr = unsafe {
        nix::libc::mmap(
            core::ptr::null_mut(),
            size,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            nix::libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };

    if addr == nix::libc::MAP_FAILED {
        Err(Error::Mmap(std::io::Error::last_os_error()))
    } else {
        Ok(addr.cast())
    }
}
