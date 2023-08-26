use bytes::Bytes;
use futures::{AsyncRead, AsyncSeek};

use crate::AsyncFileSource;

/// A trait for types that can be used a source for data.
///
/// This trait sould only ever be implemented for local (in-memory or on-disk) data sources, and is
/// design to allow some early optimizations before sending out a complete buffer out on the
/// network when storing a value.
///
/// `AsyncSource` requires data to be re-readable, which allows the process to compute the full
/// hash for the data, possibly checking for existence on a remote server, and then restart the
/// reading operation for the actual data sending. Without this requirement, we would need to
/// always keep the full data in memory, which would be problematic for larger files.
pub trait AsyncSource: AsyncRead + AsyncSeek + Unpin {
    /// Get the size of the underlying data.
    ///
    /// In effect, this prevents implementing `AsyncSource` for buffers of unknown size, which is
    /// desired.
    fn size(&self) -> u64;
}

impl AsyncSource for futures::io::Cursor<&[u8]> {
    fn size(&self) -> u64 {
        self.get_ref()
            .len()
            .try_into()
            .expect("buffers larger than 2^64 are not supported")
    }
}

impl AsyncSource for futures::io::Cursor<Vec<u8>> {
    fn size(&self) -> u64 {
        self.get_ref()
            .len()
            .try_into()
            .expect("buffers larger than 2^64 are not supported")
    }
}

impl AsyncSource for futures::io::Cursor<Bytes> {
    fn size(&self) -> u64 {
        self.get_ref()
            .len()
            .try_into()
            .expect("buffers larger than 2^64 are not supported")
    }
}

impl AsyncSource for AsyncFileSource {
    fn size(&self) -> u64 {
        self.size
    }
}
