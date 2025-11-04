use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    InvalidVersion,
    InvalidBufferSize,
    Io(std::io::Error),
    Mmap(std::io::Error),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidVersion => write!(f, "invalid version"),
            Self::InvalidBufferSize => write!(f, "invalid buffer size"),
            Self::Io(err) => write!(f, "io; err={err}"),
            Self::Mmap(err) => write!(f, "mmap; err={err}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}
