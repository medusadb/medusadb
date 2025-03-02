//! An identifier type for data blobs.

use std::{
    borrow::Cow,
    fmt::Display,
    io::{Read, Write},
    pin::Pin,
    str::FromStr,
};

use base64::Engine;
use byteorder::ReadBytesExt;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Future};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use thiserror::Error;

use crate::{
    RemoteRef,
    buf_utils::{self, read_buffer_size_with_size_len},
};

/// An error type for [``BlobIds``](``BlobId``).
#[derive(Debug, Error)]
pub enum Error {
    /// The blob id is invalid.
    #[error("invalid blob id: {0}")]
    InvalidBlobId(String),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A [``BlobId``] is an identifier for an immutable blob of data in GorgonDB.
///
/// BlobIds can be of different nature:
///
/// - A self-contained ``BlobId`` contains the data itself, and is usually reserved for smaller
///   blobs (think, 63 bytes or less). Self-contained blob ids are special in that reading them is
///   simply reading from local memory and writing them is a no-op.
/// - A remote-ref ``BlobId`` is a direct hash of the data it points to. This is the typical way of
///   storing data in GorgonDB.
/// - A ledger ``BlobId`` is a ``BlobId`` that points to a list of other blob ids and represents big
///   blobs of data separated in smaller chunks.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, DeserializeFromStr, SerializeDisplay)]
pub enum BlobId {
    /// The ``BlobId`` contains the data directly.
    ///
    /// This variant cannot hold more than 256 bytes.
    SelfContained(Cow<'static, [u8]>),

    /// The ``BlobId`` is a hash to a blob of data stored elsewhere.
    RemoteRef(RemoteRef),

    /// The ``BlobId`` is an aggregation of other blob ids.
    Ledger {
        /// The total size of the reference value.
        ref_size: u64,
        /// A `BlobId` that references the internal ledger data.
        blob_id: Box<Self>,
    },
}

impl std::fmt::Debug for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SelfContained(_) => f
                .debug_tuple("SelfContained")
                .field(&self.to_string())
                .finish(),
            Self::RemoteRef(_) => f.debug_tuple("RemoteRef").field(&self.to_string()).finish(),
            Self::Ledger { ref_size, blob_id } => f
                .debug_struct("Ledger")
                .field("ref_size", ref_size)
                .field("blob_id", blob_id)
                .finish(),
        }
    }
}
impl BlobId {
    /// The maximum size of self-contained values.
    pub const MAX_SELF_CONTAINED_SIZE: u64 = 63;

    const INFO_BITS_SELF_CONTAINED: u8 = 0x00;
    pub(crate) const INFO_BITS_REMOTE_REF: u8 = 0x01;
    const INFO_BITS_LEDGER: u8 = 0x02;

    /// Instantiate an empty ``BlobId``.
    pub fn empty() -> Self {
        Self::SelfContained(Default::default())
    }

    /// Instantiate a new self-contained ``BlobId`` referencing the specified data.
    ///
    /// If the data exceed 63 bytes, an error is returned.
    ///
    /// The data will be copied from the slice.
    pub fn self_contained(buf: impl Into<Cow<'static, [u8]>>) -> Result<Self> {
        let buf = buf.into();

        if buf.len() > 0x3f {
            Err(Error::InvalidBlobId(format!(
                "self-contained blob id can never exceed {} bytes",
                0x3f,
            )))
        } else {
            Ok(Self::SelfContained(buf))
        }
    }

    /// Get the size of the data referenced by this blob id.
    pub fn size(&self) -> u64 {
        match self {
            Self::SelfContained(raw) => raw
                .len()
                .try_into()
                .expect("self-contained data size should be representable by a u64"),
            Self::RemoteRef(remote_ref) => remote_ref.ref_size(),
            Self::Ledger { ref_size, .. } => *ref_size,
        }
    }

    /// Get the size of the resulting buffer when calling `to_vec` or `write_to`.
    pub fn buf_len(&self) -> usize {
        match self {
            Self::SelfContained(raw) => {
                buf_utils::buffer_size_len(
                    raw.len()
                        .try_into()
                        .expect("self-contained data size should be representable by a u64"),
                ) as usize
                    + raw.len()
            }
            Self::RemoteRef(remote_ref) => remote_ref.buf_len(),
            Self::Ledger { ref_size, blob_id } => {
                buf_utils::buffer_size_len(*ref_size) as usize + blob_id.buf_len()
            }
        }
    }

    /// Write the blob id to the specified writer, returning the number of bytes written.
    pub fn write_to(&self, mut w: impl Write) -> std::io::Result<usize> {
        match self {
            Self::SelfContained(raw) => {
                let cnt = buf_utils::write_buffer_size_len(
                    &mut w,
                    raw.len()
                        .try_into()
                        .expect("self-contained data size should be representable by a u8"),
                    Self::INFO_BITS_SELF_CONTAINED,
                )?;
                w.write_all(raw)?;

                Ok(cnt + raw.len())
            }
            Self::RemoteRef(remote_ref) => remote_ref.write_to(w),
            Self::Ledger { ref_size, blob_id } => {
                let cnt = buf_utils::write_buffer_size(&mut w, *ref_size, Self::INFO_BITS_LEDGER)?;

                Ok(blob_id.write_to(w)? + cnt)
            }
        }
    }

    /// Write the blob id to the specified writer, returning the number of bytes written.
    pub fn async_write_to<'w>(
        &'w self,
        mut w: impl AsyncWrite + Unpin + 'w,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<usize>> + 'w>> {
        Box::pin(async move {
            match self {
                Self::SelfContained(raw) => {
                    let cnt = buf_utils::async_write_buffer_size_len(
                        &mut w,
                        raw.len()
                            .try_into()
                            .expect("self-contained data size should be representable by a u8"),
                        Self::INFO_BITS_SELF_CONTAINED,
                    )
                    .await?;
                    w.write_all(raw).await?;

                    Ok(cnt + raw.len())
                }
                Self::RemoteRef(remote_ref) => remote_ref.async_write_to(w).await,
                Self::Ledger { ref_size, blob_id } => {
                    let cnt = buf_utils::async_write_buffer_size(
                        &mut w,
                        *ref_size,
                        Self::INFO_BITS_LEDGER,
                    )
                    .await?;

                    Ok(blob_id.async_write_to(w).await? + cnt)
                }
            }
        })
    }

    /// Return a vector of bytes representing the blob id in a non human-friendly way.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(self.buf_len());

        self.write_to(&mut res)
            .expect("writing to a memory buffer should never fail");

        res
    }

    /// Read a `BlobId` from the specified reader.
    pub fn read_from(mut r: impl Read) -> std::io::Result<Self> {
        match buf_utils::read_buffer_size_len(&mut r).map_err(|err| {
            std::io::Error::new(err.kind(), format!("failed to read reference size: {err}"))
        })? {
            (raw_size, Self::INFO_BITS_SELF_CONTAINED) => {
                let mut buf = vec![0x00; raw_size as usize];

                r.read_exact(&mut buf).map_err(|err| {
                    std::io::Error::new(err.kind(), "failed to read self-contained data")
                })?;

                Ok(Self::SelfContained(buf.into()))
            }
            (size_len, Self::INFO_BITS_REMOTE_REF) => {
                let ref_size = read_buffer_size_with_size_len(&mut r, size_len)?;
                RemoteRef::read_without_header_from(ref_size, r).map(Self::RemoteRef)
            }
            (size_len, Self::INFO_BITS_LEDGER) => {
                let ref_size = read_buffer_size_with_size_len(&mut r, size_len)?;
                let blob_id = Box::new(BlobId::read_from(r)?);

                Ok(Self::Ledger { ref_size, blob_id })
            }
            (_, info_bits) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected info bits `{info_bits:02x}` for blob id"),
            )),
        }
    }

    /// Read a `BlobId` from the specified async reader.
    pub fn async_read_from<'r>(
        mut r: impl AsyncRead + Unpin + Send + 'r,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self>> + Send + 'r>> {
        Box::pin(async move {
            match buf_utils::async_read_buffer_size_len(&mut r)
                .await
                .map_err(|err| {
                    std::io::Error::new(err.kind(), format!("failed to read reference size: {err}"))
                })? {
                (raw_size, Self::INFO_BITS_SELF_CONTAINED) => {
                    let mut buf = vec![0x00; raw_size as usize];

                    r.read_exact(&mut buf).await.map_err(|err| {
                        std::io::Error::new(err.kind(), "failed to read self-contained data")
                    })?;

                    Ok(Self::SelfContained(buf.into()))
                }
                (size_len, Self::INFO_BITS_REMOTE_REF) => {
                    let ref_size =
                        buf_utils::async_read_buffer_size_with_size_len(&mut r, size_len).await?;
                    RemoteRef::async_read_without_header_from(ref_size, r)
                        .await
                        .map(Self::RemoteRef)
                }
                (size_len, Self::INFO_BITS_LEDGER) => {
                    let ref_size =
                        buf_utils::async_read_buffer_size_with_size_len(&mut r, size_len).await?;
                    let blob_id = Box::new(BlobId::async_read_from(r).await?);

                    Ok(Self::Ledger { ref_size, blob_id })
                }
                (_, info_bits) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected info bits `{info_bits:02x}` for blob id"),
                )),
            }
        })
    }

    /// Read the `BlobId` from the specified slice.
    ///
    /// If the slice contains extra data, an error is returned.
    pub fn from_slice(buf: &[u8]) -> Result<Self> {
        let mut r = std::io::Cursor::new(buf);

        let res = Self::read_from(&mut r).map_err(|err| Error::InvalidBlobId(err.to_string()))?;

        match r.read_u8() {
            Ok(b) => Err(Error::InvalidBlobId(format!(
                "unexpected byte after blob id: 0x{b:02x}"
            ))),
            Err(err) if err.kind() != std::io::ErrorKind::UnexpectedEof => Err(
                Error::InvalidBlobId(format!("unexpected error after blob id: {err}")),
            ),
            Err(_) => Ok(res),
        }
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base64::engine::general_purpose::URL_SAFE.encode(self.to_vec()))
    }
}

impl FromStr for BlobId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw = base64::engine::general_purpose::URL_SAFE
            .decode(s)
            .map_err(|err| Error::InvalidBlobId(format!("failed to parse base64 string: {err}")))?;

        Self::read_from(std::io::Cursor::new(raw))
            .map_err(|err| Error::InvalidBlobId(err.to_string()))
    }
}

impl From<RemoteRef> for BlobId {
    fn from(value: RemoteRef) -> Self {
        Self::RemoteRef(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn test_blob_id_empty() {
        let expected = "AA=="; // Contains `vec[]`.
        let blob_id = BlobId::empty();
        assert_eq!(blob_id, BlobId::SelfContained(Default::default()));
        assert_eq!(blob_id.to_string(), expected);
        assert_eq!(blob_id.size(), 0);

        let blob_id: BlobId = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(blob_id).unwrap(), json!(expected));
    }

    #[test]
    fn test_blob_id_self_contained() {
        let expected = "AQE="; // Contains `vec[0x01]`.
        let blob_id: BlobId = expected.parse().unwrap();
        assert_eq!(blob_id, BlobId::self_contained(vec![0x01]).unwrap());
        assert_eq!(blob_id.to_string(), expected);
        assert_eq!(blob_id.size(), 1);

        let blob_id: BlobId = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(blob_id).unwrap(), json!(expected));
    }

    #[test]
    fn test_blob_id_remote_ref() {
        let expected = "QQEBSPxyH7vBcuCSX6J68Wcd4iW6knE0gCmYsQoVaKGIZSs=";
        let blob_id: BlobId = expected.parse().unwrap();
        assert!(matches!(blob_id, BlobId::RemoteRef(_)));
        assert_eq!(blob_id.to_string(), expected);
        assert_eq!(blob_id.size(), 1);

        let blob_id: BlobId = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(blob_id).unwrap(), json!(expected));
    }

    #[test]
    fn test_blob_id_ledger() {
        let first = BlobId::self_contained(vec![0x01]).unwrap();
        let second = BlobId::self_contained(vec![0x02]).unwrap();

        let mut data = Vec::with_capacity(first.buf_len() + second.buf_len());
        first.write_to(&mut data).unwrap();
        second.write_to(&mut data).unwrap();
        let inner = BlobId::self_contained(data).unwrap();

        // [0x81, 0x02, 0x04, 0x01, 0x01, 0x01, 0x02]
        //  ^     ^     ^     ^           ^
        //  |     |     |     |           \- The `second` self-contained blob id.
        //  |     |     |     |
        //  |     |     |     \- The `first` self-contained blob id.
        //  |     |     |
        //  |     |     \- The `inner` blob id that contains, as its data, two self-contained
        //  |     |        blob ids.
        //  |     |
        //  |     \- The encoded size of the aggregated data: that's 1 + 1 = 2.
        //  |
        //  \- The ledger info bit (0x03 << 6) followed by the size of the size (1).
        let expected = "gQIEAQEBAg==";
        let blob_id: BlobId = expected.parse().unwrap();
        assert_eq!(
            blob_id,
            BlobId::Ledger {
                ref_size: 2,
                blob_id: Box::new(inner),
            },
        );
        assert_eq!(blob_id.to_string(), expected);
        assert_eq!(blob_id.size(), 2);

        let blob_id: BlobId = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(blob_id).unwrap(), json!(expected));
    }

    #[tokio::test]
    async fn test_blob_id_async() {
        let expected: BlobId = "AQE=".parse().unwrap();
        let buf = expected.to_vec();
        let r = futures::io::Cursor::new(&buf);

        let blob_id = BlobId::async_read_from(r).await.unwrap();
        assert_eq!(blob_id, expected);

        let mut res = Vec::default();
        blob_id.async_write_to(&mut res).await.unwrap();

        assert_eq!(res, buf);
    }
}
