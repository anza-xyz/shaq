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

// pub fn create_and_map_file(path: impl AsRef<Path>, size: usize) -> Result<NonNull<u8>, Error> {
//     let file = create_file(path.as_ref(), size)?;
//     let mmap = map(&file, size)?;
//     NonNull::new(mmap).ok_or(Error::Mmap(std::io::Error::last_os_error()))
// }

// pub fn open_and_map_file(path: impl AsRef<Path>) -> Result<(*mut u8, usize), Error> {
//     let file = open_file(path.as_ref())?;
//     let file_size = file.metadata()?.len() as usize;
//         map(&file, file_size)
// ?
//     Ok((mmap, file_size))
// }

// fn create_file(path: impl AsRef<Path>, size: usize) -> Result<File, Error> {
//     let file = OpenOptions::new()
//         .read(true)
//         .write(true)
//         .create_new(true)
//         .open(path.as_ref())?;

//     file.set_len(size as u64)?;
//     Ok(file)
// }

// fn open_file(path: impl AsRef<Path>) -> Result<File, Error> {
//     let file = OpenOptions::new()
//         .read(true)
//         .write(true)
//         .open(path.as_ref())?;
//     Ok(file)
// }

// fn map(file: &File, size: usize) -> Result<*mut u8, Error> {
//     let addr = unsafe {
//         nix::libc::mmap(
//             core::ptr::null_mut(),
//             size,
//             nix::libc::PROT_READ | nix::libc::PROT_WRITE,
//             nix::libc::MAP_SHARED,
//             file.as_raw_fd(),
//             0,
//         )
//     };

//     if addr == nix::libc::MAP_FAILED {
//         Err(Error::Mmap(std::io::Error::last_os_error()))
//     } else {
//         Ok(addr.cast())
//     }
// }

// pub const CACHELINE_SIZE: usize = 64;
// pub const STANDARD_PAGE_SIZE: usize = 4096;
// pub const HUGE_PAGE_SIZE: usize = 1 << 21;

// /// Round up to the nearest multiple of `ROUNDING_SIZE`.
// /// This is a constant function that can be used in `const` contexts.
// ///
// /// Safety:
// ///     - `ROUNDING_SIZE` must be a power of 2.
// pub const unsafe fn pow2_round_size<const ROUNDING_SIZE: usize>(size: usize) -> usize {
//     (size + ROUNDING_SIZE - 1) & !(ROUNDING_SIZE - 1)
// }

// pub fn use_hugepages(path: impl AsRef<Path>) -> bool {
//     path.as_ref().starts_with("/mnt/hugepages")
// }

// pub fn get_rounded_file_size(path: impl AsRef<Path>, size: usize) -> usize {
//     let hugepages = use_hugepages(path.as_ref());
//     if hugepages {
//         // Safety: `HUGE_PAGE_SIZE` is a power of 2.
//         unsafe { pow2_round_size::<HUGE_PAGE_SIZE>(size) }
//     } else {
//         // Safety: `STANDARD_PAGE_SIZE` is a power of 2.
//         unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(size) }
//     }
// }

// pub const fn round_to_next_cacheline(size: usize) -> usize {
//     // SAFETY: `STANDARD_CACHELINE_SIZE` is a power of 2.
//     unsafe { pow2_round_size::<CACHELINE_SIZE>(size) }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_pow2_round_size() {
//         assert_eq!(unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(1234) }, 4096);
//         assert_eq!(unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(4096) }, 4096);
//         assert_eq!(unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(8192) }, 8192);
//         assert_eq!(
//             unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(8193) },
//             12288
//         );
//     }
// }
