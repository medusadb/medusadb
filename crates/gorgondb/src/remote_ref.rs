//! An identifier type for remote references.

use std::{
    borrow::Cow,
    fmt::Display,
    io::{Read, Write},
    str::FromStr,
};

use base64::Engine;
use byteorder::ReadBytesExt;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use thiserror::Error;

use crate::{buf_utils, BlobId, HashAlgorithm};

/// An error type for [``RemoteRefs``](``RemoteRef``).
#[derive(Debug, Error)]
pub enum Error {
    /// The remote-reference is invalid.
    #[error("invalid remote-reference: {0}")]
    InvalidRemoteRef(String),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A remote-reference to a blob of data.
///
/// In most case, you should not need to deal with this type, but rather with [`crate::BlobId`].
///
/// # Handling remote-references
///
/// Remote references are designed to be printable and - as such - parseable.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, DeserializeFromStr, SerializeDisplay)]
pub struct RemoteRef {
    pub(crate) ref_size: u64,
    pub(crate) hash_algorithm: HashAlgorithm,
    pub(crate) hash: Cow<'static, [u8]>,
}

impl std::fmt::Debug for RemoteRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RemoteRef").field(&self.to_string()).finish()
    }
}

impl RemoteRef {
    /// Compute a remote reference for a **non-empty** slice of data.
    ///
    /// The resulting `RemoteRef` should only ever be used to refer to a strictly identical blob of
    /// data.
    ///
    /// # Panics
    ///
    /// If the specified slice is empty, the call panics.
    /// If the info bits exceeds 15, the call panics.
    pub fn for_slice(buf: &[u8]) -> Self {
        assert!(!buf.is_empty(), "slice cannot be empty");

        let ref_size = buf
            .len()
            .try_into()
            .expect("slice's length should fit within an u64");
        let hash_algorithm = HashAlgorithm::Blake3;
        let hash = hash_algorithm.hash_to_vec(buf).into();

        Self {
            ref_size,
            hash_algorithm,
            hash,
        }
    }

    /// Get the size of the referenced buffer.
    pub fn ref_size(&self) -> u64 {
        self.ref_size
    }

    /// Get the hash algorithm used to hash the remote reference.
    pub fn hash_algorithm(&self) -> HashAlgorithm {
        self.hash_algorithm
    }

    /// Get the hash associated to the remote reference.
    pub fn hash(&self) -> &[u8] {
        &self.hash
    }

    /// Get the size of the resulting buffer when calling `to_vec` or `write_to`.
    pub fn buf_len(&self) -> usize {
        buf_utils::buffer_size_len(self.ref_size) as usize + 1 + self.hash.len()
    }

    /// Write the remote reference to the specified writer, returning the number of bytes written.
    pub fn write_to(&self, mut w: impl Write) -> std::io::Result<usize> {
        let count =
            buf_utils::write_buffer_size(&mut w, self.ref_size, BlobId::INFO_BITS_REMOTE_REF)?
                + 1
                + self.hash.len();
        w.write_all(&[self.hash_algorithm.into()])?;
        w.write_all(&self.hash)?;

        Ok(count)
    }

    /// Write the remote reference to the specified writer, returning the number of bytes written.
    pub async fn async_write_to(&self, mut w: impl AsyncWrite + Unpin) -> std::io::Result<usize> {
        let count =
            buf_utils::async_write_buffer_size(&mut w, self.ref_size, BlobId::INFO_BITS_REMOTE_REF)
                .await?
                + 1
                + self.hash.len();
        w.write_all(&[self.hash_algorithm.into()]).await?;
        w.write_all(&self.hash).await?;

        Ok(count)
    }

    /// Return a vector of bytes representing the remote reference in a non human-friendly way.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(self.buf_len());
        self.write_to(&mut res)
            .expect("writing to a memory buffer should never fail");

        res
    }

    pub(crate) fn read_without_header_from(
        ref_size: u64,
        mut r: impl Read,
    ) -> std::io::Result<Self> {
        let hash_algorithm: HashAlgorithm = r
            .read_u8()
            .map_err(|err| {
                std::io::Error::new(err.kind(), format!("failed to read hash algorithm: {err}"))
            })?
            .try_into()
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse hash algorithm: {err}"),
                )
            })?;

        let mut hash = vec![0x00; hash_algorithm.size()];

        r.read_exact(&mut hash).map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!(
                    "failed to read {} bytes hash of type `{hash_algorithm}`",
                    hash_algorithm.size()
                ),
            )
        })?;

        Ok(Self {
            ref_size,
            hash_algorithm,
            hash: hash.into(),
        })
    }

    pub(crate) async fn async_read_without_header_from(
        ref_size: u64,
        mut r: impl AsyncRead + Unpin,
    ) -> std::io::Result<Self> {
        let mut buf = vec![0x00; 1];
        r.read_exact(&mut buf).await.map_err(|err| {
            std::io::Error::new(err.kind(), format!("failed to read hash algorithm: {err}"))
        })?;

        let hash_algorithm: HashAlgorithm = buf[0].try_into().map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse hash algorithm: {err}"),
            )
        })?;

        let mut hash = vec![0x00; hash_algorithm.size()];

        r.read_exact(&mut hash).await.map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!(
                    "failed to read {} bytes hash of type `{hash_algorithm}`",
                    hash_algorithm.size()
                ),
            )
        })?;

        Ok(Self {
            ref_size,
            hash_algorithm,
            hash: hash.into(),
        })
    }

    /// Read the remote reference from the specified reader.
    pub fn read_from(mut r: impl Read) -> std::io::Result<Self> {
        match buf_utils::read_buffer_size(&mut r).map_err(|err| {
            std::io::Error::new(err.kind(), format!("failed to read reference size: {err}"))
        })? {
            (ref_size, BlobId::INFO_BITS_REMOTE_REF) => Self::read_without_header_from(ref_size, r),
            (_, info_bits) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid non-zero info bits `0x{info_bits:02x}`"),
            )),
        }
    }

    /// Read the remote reference from the specified async reader.
    pub async fn async_read_from(mut r: impl AsyncRead + Unpin) -> std::io::Result<Self> {
        match buf_utils::async_read_buffer_size(&mut r)
            .await
            .map_err(|err| {
                std::io::Error::new(err.kind(), format!("failed to read reference size: {err}"))
            })? {
            (ref_size, BlobId::INFO_BITS_REMOTE_REF) => {
                Self::async_read_without_header_from(ref_size, r).await
            }
            (_, info_bits) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid non-zero info bits `0x{info_bits:02x}`"),
            )),
        }
    }

    /// Read the remote reference from the specified slice.
    ///
    /// If the slice contains extra data, an error is returned.
    pub fn from_slice(buf: &[u8]) -> Result<Self> {
        let mut r = std::io::Cursor::new(buf);

        let res =
            Self::read_from(&mut r).map_err(|err| Error::InvalidRemoteRef(err.to_string()))?;

        match r.read_u8() {
            Ok(b) => Err(Error::InvalidRemoteRef(format!(
                "unexpected byte after remote ref: 0x{b:02x}"
            ))),
            Err(err) if err.kind() != std::io::ErrorKind::UnexpectedEof => Err(
                Error::InvalidRemoteRef(format!("unexpected error after remote ref: {err}")),
            ),
            Err(_) => Ok(res),
        }
    }
}

impl Display for RemoteRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base64::engine::general_purpose::URL_SAFE.encode(self.to_vec()))
    }
}

impl FromStr for RemoteRef {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw = base64::engine::general_purpose::URL_SAFE
            .decode(s)
            .map_err(|err| {
                Error::InvalidRemoteRef(format!("failed to parse base64 string: {err}"))
            })?;

        Self::read_from(std::io::Cursor::new(raw))
            .map_err(|err| Error::InvalidRemoteRef(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn test_remote_ref() {
        let expected = "QQEBSPxyH7vBcuCSX6J68Wcd4iW6knE0gCmYsQoVaKGIZSs=";
        let remote_ref: RemoteRef = expected.parse().unwrap();
        assert_eq!(remote_ref.to_string(), expected);

        let remote_ref: RemoteRef = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(remote_ref).unwrap(), json!(expected));
    }

    #[tokio::test]
    async fn test_remote_ref_async() {
        let expected = RemoteRef::for_slice(&[0x01]);
        let buf = expected.to_vec();
        let r = futures::io::Cursor::new(&buf);

        let remote_ref = RemoteRef::async_read_from(r).await.unwrap();
        assert_eq!(remote_ref, expected);

        let mut res = Vec::default();
        remote_ref.async_write_to(&mut res).await.unwrap();

        assert_eq!(res, buf);
    }

    #[test]
    fn test_remote_ref_for_slice() {
        let remote_ref = RemoteRef::for_slice(&[0x01]);

        assert_eq!(
            remote_ref.to_vec(),
            &[
                0x41, 0x01, 0x01, 0x48, 0xfc, 0x72, 0x1f, 0xbb, 0xc1, 0x72, 0xe0, 0x92, 0x5f, 0xa2,
                0x7a, 0xf1, 0x67, 0x1d, 0xe2, 0x25, 0xba, 0x92, 0x71, 0x34, 0x80, 0x29, 0x98, 0xb1,
                0x0a, 0x15, 0x68, 0xa1, 0x88, 0x65, 0x2b,
            ]
        );

        assert_eq!(remote_ref.ref_size(), 1);
        assert_eq!(remote_ref.hash_algorithm(), HashAlgorithm::Blake3);
        assert_eq!(
            remote_ref.hash(),
            &[
                0x48, 0xfc, 0x72, 0x1f, 0xbb, 0xc1, 0x72, 0xe0, 0x92, 0x5f, 0xa2, 0x7a, 0xf1, 0x67,
                0x1d, 0xe2, 0x25, 0xba, 0x92, 0x71, 0x34, 0x80, 0x29, 0x98, 0xb1, 0x0a, 0x15, 0x68,
                0xa1, 0x88, 0x65, 0x2b,
            ]
        );

        let other = RemoteRef::from_slice(&remote_ref.to_vec()).unwrap();
        assert_eq!(remote_ref, other);

        let remote_ref = RemoteRef::for_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04,
        ]);

        assert_eq!(
            remote_ref.to_vec(),
            &[
                0x41, 0x20, 0x01, 0x9c, 0x4d, 0x78, 0xc2, 0xd6, 0x5a, 0x8e, 0x17, 0x2b, 0x68, 0x4e,
                0xec, 0xac, 0x47, 0x05, 0x24, 0x15, 0x57, 0xe8, 0x60, 0xe9, 0xa0, 0x41, 0x56, 0x37,
                0x36, 0xfc, 0x16, 0x02, 0xac, 0x1f, 0x08,
            ]
        );

        assert_eq!(remote_ref.ref_size(), 32);
        assert_eq!(remote_ref.hash_algorithm(), HashAlgorithm::Blake3);
        assert_eq!(
            remote_ref.hash(),
            &[
                0x9c, 0x4d, 0x78, 0xc2, 0xd6, 0x5a, 0x8e, 0x17, 0x2b, 0x68, 0x4e, 0xec, 0xac, 0x47,
                0x05, 0x24, 0x15, 0x57, 0xe8, 0x60, 0xe9, 0xa0, 0x41, 0x56, 0x37, 0x36, 0xfc, 0x16,
                0x02, 0xac, 0x1f, 0x08,
            ]
        );

        let other = RemoteRef::from_slice(&remote_ref.to_vec()).unwrap();
        assert_eq!(remote_ref, other);

        let remote_ref = RemoteRef::for_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
            0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,
        ]);

        assert_eq!(
            remote_ref.to_vec(),
            &[
                0x42, 0x02, 0x00, 0x01, 0x44, 0x80, 0x4d, 0x04, 0x99, 0xdb, 0x3a, 0x70, 0x27, 0x3a,
                0xed, 0x86, 0xec, 0xdc, 0xa6, 0x92, 0x57, 0xa1, 0xe6, 0x5b, 0x8e, 0xd0, 0xb8, 0x26,
                0x50, 0xbd, 0x5b, 0xf8, 0x6a, 0x2a, 0x8d, 0x39,
            ]
        );

        assert_eq!(remote_ref.ref_size(), 512);
        assert_eq!(remote_ref.hash_algorithm(), HashAlgorithm::Blake3);
        assert_eq!(
            remote_ref.hash(),
            &[
                0x44, 0x80, 0x4d, 0x04, 0x99, 0xdb, 0x3a, 0x70, 0x27, 0x3a, 0xed, 0x86, 0xec, 0xdc,
                0xa6, 0x92, 0x57, 0xa1, 0xe6, 0x5b, 0x8e, 0xd0, 0xb8, 0x26, 0x50, 0xbd, 0x5b, 0xf8,
                0x6a, 0x2a, 0x8d, 0x39,
            ]
        );

        let other = RemoteRef::from_slice(&remote_ref.to_vec()).unwrap();
        assert_eq!(remote_ref, other);
    }
}
