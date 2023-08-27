use std::{
    fmt::Display,
    io::{Read, Write},
    pin::Pin,
    str::FromStr,
};

use byteorder::ReadBytesExt;
use bytes::Bytes;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Future};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use thiserror::Error;

use crate::{
    buf_utils::{self, read_buffer_size_with_size_len},
    RemoteRef,
};

/// An error type for [``Cairns``](``Cairn``).
#[derive(Debug, Error)]
pub enum Error {
    /// The cairn is invalid.
    #[error("invalid cairn: {0}")]
    InvalidCairn(String),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A [``Cairn``](https://en.wikipedia.org/wiki/Cairn) is a marker for an unalterable piece of
/// information - or put otherwise: an identifier for an immutable blob of data in GorgonDB.
///
/// Cairns can be of different nature:
///
/// - A self-contained ``Cairn`` contains the data itself, and is usually reserved for smaller
/// blobs (think, 63 bytes or less). Self-contained cairns are special in that reading them is
/// simply reading from local memory and writing them is a no-op.
/// - A remote-ref ``Cairn`` is a direct hash of the data it points to. This is the typical way of
/// storing data in GorgonDB.
/// - A ledger ``Cairn`` is a ``Cairn`` that points to a list of other cairns and represents big
/// blobs of data separated in smaller chunks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, DeserializeFromStr, SerializeDisplay)]
pub enum Cairn {
    /// The ``Cairn`` contains the data directly.
    ///
    /// This variant cannot hold more than 256 bytes.
    SelfContained(Bytes),

    /// The ``Cairn`` is a hash to a blob of data stored elsewhere.
    RemoteRef(RemoteRef),

    /// The ``Cairn`` is an aggregation of other cairns.
    Ledger { ref_size: u64, cairn: Box<Self> },
}

impl Cairn {
    /// The maximum size of self-contained values.
    pub const MAX_SELF_CONTAINED_SIZE: u64 = 63;

    const INFO_BITS_SELF_CONTAINED: u8 = 0x00;
    pub(crate) const INFO_BITS_REMOTE_REF: u8 = 0x01;
    const INFO_BITS_LEDGER: u8 = 0x02;

    /// Instantiate an empty ``Cairn``.
    pub fn empty() -> Self {
        Self::SelfContained(Default::default())
    }

    /// Instantiate a new self-contained ``Cairn`` referencing the specified data.
    ///
    /// If the data exceed 63 bytes, an error is returned.
    ///
    /// The data will be copied from the slice.
    pub fn self_contained(buf: impl Into<Bytes>) -> Result<Self> {
        let buf = buf.into();

        if buf.len() > 0x3f {
            Err(Error::InvalidCairn(format!(
                "self-contained cairn can never exceed {} bytes",
                0x3f,
            )))
        } else {
            Ok(Self::SelfContained(buf))
        }
    }

    /// Get the size of the data referenced by this cairn.
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
            Self::Ledger { ref_size, cairn } => {
                buf_utils::buffer_size_len(*ref_size) as usize + cairn.buf_len()
            }
        }
    }

    /// Write the cairn to the specified writer, returning the number of bytes written.
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
            Self::Ledger { ref_size, cairn } => {
                let cnt = buf_utils::write_buffer_size(&mut w, *ref_size, Self::INFO_BITS_LEDGER)?;

                Ok(cairn.write_to(w)? + cnt)
            }
        }
    }

    /// Write the cairn to the specified writer, returning the number of bytes written.
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
                Self::Ledger { ref_size, cairn } => {
                    let cnt = buf_utils::async_write_buffer_size(
                        &mut w,
                        *ref_size,
                        Self::INFO_BITS_LEDGER,
                    )
                    .await?;

                    Ok(cairn.async_write_to(w).await? + cnt)
                }
            }
        })
    }

    /// Return a vector of bytes representing the cairn in a non human-friendly way.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(self.buf_len());

        self.write_to(&mut res)
            .expect("writing to a memory buffer should never fail");

        res
    }

    /// Read a `Cairn` from the specified reader.
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
                let cairn = Box::new(Cairn::read_from(r)?);

                Ok(Self::Ledger { ref_size, cairn })
            }
            (_, info_bits) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected info bits `{info_bits:02x}` for cairn"),
            )),
        }
    }

    /// Read a `Cairn` from the specified async reader.
    pub fn async_read_from<'r>(
        mut r: impl AsyncRead + Unpin + 'r,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self>> + 'r>> {
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
                    let cairn = Box::new(Cairn::async_read_from(r).await?);

                    Ok(Self::Ledger { ref_size, cairn })
                }
                (_, info_bits) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected info bits `{info_bits:02x}` for cairn"),
                )),
            }
        })
    }

    /// Read the `Cairn` from the specified slice.
    ///
    /// If the slice contains extra data, an error is returned.
    pub fn from_slice(buf: &[u8]) -> Result<Self> {
        let mut r = std::io::Cursor::new(buf);

        let res = Self::read_from(&mut r).map_err(|err| Error::InvalidCairn(err.to_string()))?;

        match r.read_u8() {
            Ok(b) => Err(Error::InvalidCairn(format!(
                "unexpected byte after cairn: 0x{b:02x}"
            ))),
            Err(err) if err.kind() != std::io::ErrorKind::UnexpectedEof => Err(
                Error::InvalidCairn(format!("unexpected error after cairn: {err}")),
            ),
            Err(_) => Ok(res),
        }
    }
}

impl Display for Cairn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base85::encode(&self.to_vec()))
    }
}

impl FromStr for Cairn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw = base85::decode(s)
            .map_err(|err| Error::InvalidCairn(format!("failed to parse base85 string: {err}")))?;

        Self::read_from(std::io::Cursor::new(raw))
            .map_err(|err| Error::InvalidCairn(err.to_string()))
    }
}

impl From<RemoteRef> for Cairn {
    fn from(value: RemoteRef) -> Self {
        Self::RemoteRef(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn test_cairn_empty() {
        let expected = "00"; // Contains `vec[]`.
        let cairn = Cairn::empty();
        assert_eq!(cairn, Cairn::SelfContained(Bytes::new()));
        assert_eq!(cairn.to_string(), expected);
        assert_eq!(cairn.size(), 0);

        let cairn: Cairn = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(cairn).unwrap(), json!(expected));
    }

    #[test]
    fn test_cairn_self_contained() {
        let expected = "0Ra"; // Contains `vec[0x01]`.
        let cairn: Cairn = expected.parse().unwrap();
        assert_eq!(cairn, Cairn::self_contained(vec![0x01]).unwrap());
        assert_eq!(cairn.to_string(), expected);
        assert_eq!(cairn.size(), 1);

        let cairn: Cairn = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(cairn).unwrap(), json!(expected));
    }

    #[test]
    fn test_cairn_remote_ref() {
        let expected = "K>-0s{Bj?=!E)e|U!r>PXC2}tx{`4;fGL=<3KeLfh-E7";
        let cairn: Cairn = expected.parse().unwrap();
        assert!(matches!(cairn, Cairn::RemoteRef(_)));
        assert_eq!(cairn.to_string(), expected);
        assert_eq!(cairn.size(), 1);

        let cairn: Cairn = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(cairn).unwrap(), json!(expected));
    }

    #[test]
    fn test_cairn_ledger() {
        let first = Cairn::self_contained(vec![0x01]).unwrap();
        let second = Cairn::self_contained(vec![0x02]).unwrap();

        let mut data = Vec::with_capacity(first.buf_len() + second.buf_len());
        first.write_to(&mut data).unwrap();
        second.write_to(&mut data).unwrap();
        let inner = Cairn::self_contained(data).unwrap();

        // [0x81, 0x02, 0x04, 0x01, 0x01, 0x01, 0x02]
        //  ^     ^     ^     ^           ^
        //  |     |     |     |           \- The `second` self-contained cairn.
        //  |     |     |     |
        //  |     |     |     \- The `first` self-contained cairn.
        //  |     |     |
        //  |     |     \- The `inner` cairn that contains, as its data, two self-contained
        //  |     |        cairns.
        //  |     |
        //  |     \- The encoded size of the aggregated data: that's 1 + 1 = 2.
        //  |
        //  \- The ledger info bit (0x03 << 6) followed by the size of the size (1).
        let expected = "fdT{p0RaL";
        let cairn: Cairn = expected.parse().unwrap();
        assert_eq!(
            cairn,
            Cairn::Ledger {
                ref_size: 2,
                cairn: Box::new(inner),
            },
        );
        assert_eq!(cairn.to_string(), expected);
        assert_eq!(cairn.size(), 2);

        let cairn: Cairn = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(cairn).unwrap(), json!(expected));
    }

    #[tokio::test]
    async fn test_cairn_async() {
        let expected: Cairn = "0Ra".parse().unwrap();
        let buf = expected.to_vec();
        let r = futures::io::Cursor::new(&buf);

        let cairn = Cairn::async_read_from(r).await.unwrap();
        assert_eq!(cairn, expected);

        let mut res = Vec::default();
        cairn.async_write_to(&mut res).await.unwrap();

        assert_eq!(res, buf);
    }
}
