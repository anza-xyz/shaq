use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    InvalidVersion { expected: u16, actual: u16 },
    InvalidBufferSize,
    Io(std::io::Error),
    Mmap(std::io::Error),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidVersion { expected, actual } => write!(
                f,
                "invalid version; expected={}.{}; found={}.{}",
                expected >> 8,
                expected & 0xFF,
                actual >> 8,
                actual & 0xFF,
            ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_version_display() {
        let expected: u16 = 1u16 << 8; // 1.0
        let actual: u16 = (3u16 << 8) | 7; // 3.7
        let err = Error::InvalidVersion { expected, actual };
        assert_eq!(err.to_string(), "invalid version; expected=1.0; found=3.7");
    }
}
