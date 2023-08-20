use std::{fmt::Display, str::FromStr};

use serde_with::{DeserializeFromStr, SerializeDisplay};
use thiserror::Error;

/// An error type for [``HashAlgorithms``](``HashAlgorithm``).
#[derive(Debug, Error)]
pub enum Error {
    /// The specified hash algorithm is unknown.
    #[error("unknown hash algorithm: {0}")]
    UnknownHashAlgorithm(String),

    /// The hash algorithm is invalid.
    #[error("invalid hash algorithm: {0}")]
    InvalidHashAlgorithm(u8),
}

/// A hash algorithm.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SerializeDisplay, DeserializeFromStr,
)]
#[repr(u8)]
pub enum HashAlgorithm {
    /// Hash using the [Blake 3](`https://en.wikipedia.org/wiki/BLAKE_(hash_function)`) algorithm.
    Blake3 = Self::BLAKE3,
}

impl FromStr for HashAlgorithm {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "blake3" => Ok(Self::Blake3),
            _ => Err(Error::UnknownHashAlgorithm(s.to_owned())),
        }
    }
}

impl Display for HashAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Blake3 => "blake3",
        })
    }
}

impl TryFrom<u8> for HashAlgorithm {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            Self::BLAKE3 => Ok(Self::Blake3),
            _ => Err(Error::InvalidHashAlgorithm(value)),
        }
    }
}

impl From<HashAlgorithm> for u8 {
    fn from(value: HashAlgorithm) -> Self {
        match value {
            HashAlgorithm::Blake3 => HashAlgorithm::BLAKE3,
        }
    }
}

impl HashAlgorithm {
    const BLAKE3: u8 = 1;

    /// Hash the given slice using the algorithm.
    pub fn hash_to(&self, mut w: impl std::io::Write, buf: &[u8]) -> std::io::Result<()> {
        w.write_all(blake3::hash(buf).as_bytes().as_slice())
    }

    /// Get the size of the resulting hash.
    pub fn size(&self) -> usize {
        match self {
            Self::Blake3 => blake3::OUT_LEN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_hash_algorithm() {
        let alg: HashAlgorithm = "blake3".parse().unwrap();
        assert_eq!(alg.to_string(), "blake3");

        let alg: HashAlgorithm = serde_json::from_value(json!("blake3")).unwrap();
        assert_eq!(serde_json::to_value(alg).unwrap(), json!("blake3"));
    }
}
