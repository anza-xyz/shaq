use crate::error::Error;
use std::{fs::File, ptr::NonNull, sync::Arc};

pub(crate) const MINIMUM_REGION_ALIGNMENT: usize = 4096;

pub(crate) struct Region {
    addr: NonNull<u8>,
    size: usize,
    backing: RegionBacking,
}

impl Region {
    pub(crate) fn map_file(file: &File, size: usize) -> Result<Arc<Self>, Error> {
        let addr = map_file(file, size)?;
        validate_region_alignment(addr)?;
        Ok(Arc::new(Self {
            addr,
            size,
            backing: RegionBacking::MappedFile,
        }))
    }

    pub(crate) fn alloc(size: usize) -> Result<Arc<Self>, Error> {
        let layout = std::alloc::Layout::from_size_align(size, MINIMUM_REGION_ALIGNMENT)
            .map_err(|_| Error::InvalidBufferSize)?;
        let addr = {
            // SAFETY: layout is valid and non-zero.
            let addr = unsafe { std::alloc::alloc_zeroed(layout) };
            NonNull::new(addr).ok_or(Error::Allocation(layout))?
        };

        assert_eq!(addr.as_ptr().align_offset(MINIMUM_REGION_ALIGNMENT), 0);

        Ok(Arc::new(Self {
            addr,
            size,
            backing: RegionBacking::Heap(layout),
        }))
    }

    pub(crate) fn addr(&self) -> NonNull<u8> {
        self.addr
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        match self.backing {
            RegionBacking::MappedFile => {
                // SAFETY: addr and size were produced by a successful map_file call.
                unsafe { unmap_file(self.addr, self.size) };
            }
            RegionBacking::Heap(layout) => {
                // SAFETY: addr was allocated with this exact layout in `alloc`.
                unsafe { std::alloc::dealloc(self.addr.as_ptr(), layout) };
            }
        }
    }
}

enum RegionBacking {
    MappedFile,
    Heap(std::alloc::Layout),
}

// SAFETY: The mapped memory is shared (MAP_SHARED / file-backed) and access
// is synchronized by the queue protocol built on top of it.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

fn validate_region_alignment(addr: NonNull<u8>) -> Result<(), Error> {
    let actual = addr.as_ptr().align_offset(MINIMUM_REGION_ALIGNMENT);
    if actual != 0 {
        return Err(Error::InvalidRegionAlignment {
            minimum: MINIMUM_REGION_ALIGNMENT,
            actual,
        });
    }

    Ok(())
}

/// Maps a file into memory.
#[cfg(unix)]
fn map_file(file: &File, size: usize) -> Result<NonNull<u8>, Error> {
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
unsafe fn unmap_file(addr: NonNull<u8>, size: usize) {
    let _ = unsafe { libc::munmap(addr.as_ptr().cast(), size) };
}

/// Maps a file into memory.
#[cfg(windows)]
fn map_file(file: &File, size: usize) -> Result<NonNull<u8>, Error> {
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

    if mmap.Value.is_null() {
        let err = Error::Mmap(std::io::Error::last_os_error());

        unsafe { CloseHandle(mapping) };

        Err(err)
    } else {
        unsafe {
            CloseHandle(mapping);
        }

        Ok(NonNull::new(mmap.Value.cast()).expect("already checked for null"))
    }
}

/// Unmaps a previously mapped file view.
#[cfg(windows)]
unsafe fn unmap_file(addr: NonNull<u8>, _size: usize) {
    use windows_sys::Win32::System::Memory::{UnmapViewOfFile, MEMORY_MAPPED_VIEW_ADDRESS};

    let _ = unsafe {
        UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
            Value: addr.cast().as_ptr(),
        })
    };
}

#[cfg(test)]
pub(crate) fn create_temp_shmem_file() -> Result<File, Error> {
    use std::fs::OpenOptions;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let temp_dir = std::env::temp_dir();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = temp_dir.join(format!("rts-alloc-{n}.tmp"));

    let mut open_options = OpenOptions::new();
    open_options.read(true).write(true).create_new(true);

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_TEMPORARY, FILE_FLAG_DELETE_ON_CLOSE,
        };

        open_options
            .attributes(FILE_ATTRIBUTE_TEMPORARY)
            .custom_flags(FILE_FLAG_DELETE_ON_CLOSE);
    }

    let open_result = open_options.open(&path);

    match open_result {
        Ok(file) => {
            #[cfg(unix)]
            {
                std::fs::remove_file(&path)?;
            }
            Ok(file)
        }
        Err(err) => Err(Error::Io(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_is_minimum_region_aligned() {
        let file = create_temp_shmem_file().expect("temp file");
        file.set_len(MINIMUM_REGION_ALIGNMENT as u64)
            .expect("set len");

        let region = Region::map_file(&file, MINIMUM_REGION_ALIGNMENT).expect("map file");
        assert_eq!(
            region
                .addr()
                .as_ptr()
                .align_offset(MINIMUM_REGION_ALIGNMENT),
            0
        );
    }

    #[test]
    fn test_alloc_region_is_4096_aligned() {
        let region = Region::alloc(MINIMUM_REGION_ALIGNMENT * 2).expect("allocation failed");
        assert_eq!(
            region
                .addr()
                .as_ptr()
                .align_offset(MINIMUM_REGION_ALIGNMENT),
            0
        );
        assert_eq!(region.size(), MINIMUM_REGION_ALIGNMENT * 2);
    }

    #[test]
    fn test_alloc_region_accepts_non_4096_multiple() {
        let region = Region::alloc(MINIMUM_REGION_ALIGNMENT + 1).expect("allocation failed");
        assert_eq!(
            region
                .addr()
                .as_ptr()
                .align_offset(MINIMUM_REGION_ALIGNMENT),
            0
        );
        assert_eq!(region.size(), MINIMUM_REGION_ALIGNMENT + 1);
    }
}
