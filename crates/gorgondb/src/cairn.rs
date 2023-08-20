use std::{fmt::Display, str::FromStr};

use byteorder::ReadBytesExt;
use bytes::Bytes;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use thiserror::Error;

use crate::{buf_utils, RemoteRef};

/// An error type for [``Cairns``](``Cairn``).
#[derive(Debug, Error)]
pub enum Error {
    /// The cairn is invalid.
    #[error("invalid cairn: {0}")]
    InvalidCairn(String),
}

/// A [``Cairn``](https://en.wikipedia.org/wiki/Cairn) is a marker for an unalterable piece of
/// information - or put otherwise: an identifier for an immutable blob of data in GorgonDB.
///
/// Cairns can be of different nature:
///
/// - A self-contained ``Cairn`` contains the data itself, and is usually reserved for smaller
/// blobs (think, 32 bytes or less). Self-contained cairns are special in that reading them is
/// simply reading from local memory and writing them is a no-op.
/// - A remote-ref ``Cairn`` is a direct hash of the data it points to. This is the typical way of
/// storing data in GorgonDB.
/// - A ledger ``Cairn`` is a ``Cairn`` that points to a list of other cairns and represents big
/// blobs of data separated in smaller chunks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, DeserializeFromStr, SerializeDisplay)]
pub enum Cairn {
    /// The ``Cairn`` contains the data directly.
    ///
    /// Using this variant with data bigger than the recommended 32 bytes limit is ill-advised.
    SelfContained(Bytes),

    /// The ``Cairn`` is a hash to a blob of data stored elsewhere.
    RemoteRef(RemoteRef),
}

impl Cairn {
    /// Instantiate an empty ``Cairn``.
    pub fn empty() -> Self {
        Self::SelfContained(vec![0x00].into())
    }

    /// Instantiate a new self-contained ``Cairn`` referencing the specified data.
    ///
    /// No check is made ot ensure that the specified data is small enough. It is **highly
    /// recommended** to never exceed 32 bytes for self-contained [``Cairns``](``Cairn``).
    ///
    /// The data will be copied from the slice.
    pub fn self_contained(buf: &[u8]) -> Self {
        let mut raw = Vec::with_capacity(buf.len() + 1);
        raw.push(0x00);
        raw.extend_from_slice(buf);

        Self::SelfContained(raw.into())
    }

    /// Get the size of the data referenced by this cairn.
    pub fn size(&self) -> u64 {
        match self {
            Self::SelfContained(raw) => (raw.len() - 1)
                .try_into()
                .expect("self-contained data must not exceed the size of an u64"),
            Self::RemoteRef(remote_ref) => remote_ref.ref_size(),
        }
    }
}

impl Display for Cairn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base85::encode(self.as_ref()))
    }
}

impl FromStr for Cairn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw: Bytes = base85::decode(s)
            .map_err(|err| Error::InvalidCairn(format!("failed to parse base85 string: {err}")))?
            .into();

        raw.try_into()
    }
}

impl AsRef<[u8]> for Cairn {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::SelfContained(raw) => raw.as_ref(),
            Self::RemoteRef(remote_ref) => remote_ref.as_ref(),
        }
    }
}

impl TryFrom<Bytes> for Cairn {
    type Error = Error;

    fn try_from(raw: Bytes) -> Result<Self, Self::Error> {
        let mut r = std::io::Cursor::new(&raw);

        match buf_utils::read_buffer_size(&mut r)
            .map_err(|err| Error::InvalidCairn(format!("failed to read reference size: {err}")))?
        {
            (Some(ref_size), _info_bits) => {
                let hash_algorithm = r
                    .read_u8()
                    .map_err(|err| Error::InvalidCairn(format!("failed to read algorithm: {err}")))?
                    .try_into()
                    .map_err(|err| {
                        Error::InvalidCairn(format!("failed to parse algorithm: {err}"))
                    })?;

                let remote_ref = RemoteRef {
                    ref_size,
                    hash_algorithm,
                    raw,
                };

                Ok(Self::RemoteRef(remote_ref))
            }
            (None, _) => Ok(Self::SelfContained(raw)),
        }
    }
}

impl From<Cairn> for Bytes {
    fn from(value: Cairn) -> Self {
        match value {
            Cairn::SelfContained(raw) => raw,
            Cairn::RemoteRef(remote_ref) => remote_ref.into(),
        }
    }
}

impl TryFrom<&'_ [u8]> for Cairn {
    type Error = Error;

    fn try_from(value: &'_ [u8]) -> Result<Self, Self::Error> {
        Bytes::copy_from_slice(value).try_into()
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
        assert_eq!(cairn.to_string(), expected);
        assert!(matches!(cairn, Cairn::SelfContained(_)));
        assert_eq!(cairn.size(), 0);

        let cairn: Cairn = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(cairn).unwrap(), json!(expected));
    }

    #[test]
    fn test_cairn_self_contained() {
        let expected = "009"; // Contains `vec[0x01]`.
        let cairn: Cairn = expected.parse().unwrap();
        assert_eq!(cairn.to_string(), expected);
        assert!(matches!(cairn, Cairn::SelfContained(_)));
        assert_eq!(cairn.size(), 1);

        let cairn: Cairn = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(cairn).unwrap(), json!(expected));
    }

    #[test]
    fn test_cairn_remote_ref() {
        let expected = "0RaI>{Bj?=!E)e|U!r>PXC2}tx{`4;fGL=<3KeLfh-E7";
        let cairn: Cairn = expected.parse().unwrap();
        assert_eq!(cairn.to_string(), expected);
        assert!(matches!(cairn, Cairn::RemoteRef(_)));
        assert_eq!(cairn.size(), 1);

        let cairn: Cairn = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(serde_json::to_value(cairn).unwrap(), json!(expected));
    }
}
