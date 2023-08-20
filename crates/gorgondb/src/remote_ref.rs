use std::borrow::Cow;

/// A remote-reference to a blob of data.
///
/// In most case, you should not need to deal with this type, but rather with [`crate::Cairn`].
#[derive(Debug, Clone)]
pub struct RemoteRef(Cow<'static, [u8]>);
