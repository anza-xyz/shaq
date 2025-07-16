#[derive(Debug)]
pub enum Error {
    InvalidBufferSize,
    Io(std::io::Error),
    Mmap(std::io::Error),
    FileSizeMismatch { expected: usize, actual: usize },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}
