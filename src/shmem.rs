use crate::error::Error;
use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
    ptr::NonNull,
};

pub fn create_and_map_file(path: impl AsRef<Path>, size: usize) -> Result<NonNull<u8>, Error> {
    let file = create_file(path.as_ref(), size)?;
    let mmap = map(&file, size)?;
    NonNull::new(mmap).ok_or(Error::Mmap(std::io::Error::last_os_error()))
}

pub fn open_and_map_file(path: impl AsRef<Path>) -> Result<(NonNull<u8>, usize), Error> {
    let file = open_file(path.as_ref())?;
    let file_size = file.metadata()?.len() as usize;
    let mmap = map(&file, file_size)?;
    let ptr = NonNull::new(mmap).ok_or(Error::Mmap(std::io::Error::last_os_error()))?;
    Ok((ptr, file_size))
}

fn create_file(path: impl AsRef<Path>, size: usize) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path.as_ref())?;

    file.set_len(size as u64)?;
    Ok(file)
}

fn open_file(path: impl AsRef<Path>) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.as_ref())?;
    Ok(file)
}

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
