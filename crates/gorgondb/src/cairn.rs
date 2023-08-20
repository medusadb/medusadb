use std::borrow::Cow;

use crate::RemoteRef;

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
#[derive(Debug, Clone)]
pub enum Cairn {
    /// The ``Cairn`` contains the data directly.
    ///
    /// Using this variant with data bigger than the recommended 32 bytes limit is ill-advised.
    SelfContained(Cow<'static, [u8]>),

    /// The ``Cairn`` is a hash to a blob of data stored elsewhere.
    RemoteRef(RemoteRef),
}
