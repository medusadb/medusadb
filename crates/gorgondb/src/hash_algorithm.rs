//! A hash algorithm.

use std::{fmt::Display, str::FromStr};

use futures::{AsyncRead, AsyncReadExt};
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

    /// Hash the given slice to a vector of bytes.
    pub fn hash_to_vec(&self, buf: &[u8]) -> Vec<u8> {
        let mut res = Vec::with_capacity(self.size());

        self.hash_to(&mut res, buf)
            .expect("hashing to a memory buffer should never fail");

        res
    }

    /// Hash the contents of an `AsyncRead` to the specified `Write`.
    pub async fn async_hash_to(
        &self,
        mut w: impl std::io::Write,
        mut r: impl AsyncRead + Unpin,
    ) -> std::io::Result<()> {
        match self {
            Self::Blake3 => {
                let mut hasher = blake3::Hasher::new();
                // Attempt to read 16K of data: this allows SIMD optimizations on Blake3 algorithms.
                let mut buf = vec![0; 16 * 1024];

                loop {
                    match r.read(&mut buf).await? {
                        0 => break w.write_all(hasher.finalize().as_bytes().as_slice()),
                        count => {
                            hasher.update(&buf[0..count]);
                            buf.clear();
                        }
                    }
                }
            }
        }
    }

    /// Hash the contents of an `AsyncRead` to a vector.
    pub async fn async_hash_to_vec(&self, r: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
        let mut res = Vec::with_capacity(self.size());

        self.async_hash_to(&mut res, r).await?;

        Ok(res)
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

    #[tokio::test]
    async fn test_hash_algorithm() {
        let alg: HashAlgorithm = "blake3".parse().unwrap();
        assert_eq!(alg.to_string(), "blake3");

        let alg: HashAlgorithm = serde_json::from_value(json!("blake3")).unwrap();
        assert_eq!(serde_json::to_value(alg).unwrap(), json!("blake3"));

        let expected = &[
            177, 119, 236, 27, 242, 109, 251, 59, 112, 16, 212, 115, 230, 212, 71, 19, 178, 155,
            118, 91, 153, 198, 230, 14, 203, 250, 231, 66, 222, 73, 101, 67,
        ];

        let hash = HashAlgorithm::Blake3.hash_to_vec(&[1, 2, 3]);
        assert_eq!(hash, expected);

        let hash = HashAlgorithm::Blake3
            .async_hash_to_vec(&[1u8, 2, 3][..])
            .await
            .unwrap();

        assert_eq!(hash, expected);
    }
}
