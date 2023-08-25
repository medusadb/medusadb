//! Provides blobs fragmentation methods.

mod fastcdc;

use bytes::Bytes;
use futures::{AsyncRead, TryStream};

pub use self::fastcdc::Fastcdc;

/// A fragmentation error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A Fastcdc error happened.
    #[error("fastcdc: {0}")]
    Fastcdc(#[from] ::fastcdc::v2020::Error),
}

/// A convenience `Result` type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A method of fragmentation for bigger blobs of data.
#[derive(Debug, Clone)]
pub enum FragmentationMethod {
    /// Use the `FastCDC` [rolling hash](`https://en.wikipedia.org/wiki/Rolling_hash`) algorithm to
    /// split bigger blobs into pieces.
    Fastcdc(Fastcdc),
}

impl Default for FragmentationMethod {
    fn default() -> Self {
        Self::Fastcdc(Default::default())
    }
}

impl FragmentationMethod {
    /// Fragment the specified source buffer asynchronously.
    pub fn fragment<'r>(
        &self,
        r: impl AsyncRead + Unpin + 'r,
    ) -> impl TryStream<Ok = Bytes, Error = Error> + 'r {
        match self {
            Self::Fastcdc(fastcdc) => fastcdc.fragment(r),
        }
    }
}

#[cfg(test)]
mod tests {
    #[macro_export]
    macro_rules! test_fragmentation {
        ($fragment_fn:expr, $sizes:expr) => {
            let fixture = env!("CARGO_MANIFEST_DIR").to_owned()
                + "/src/fragmentation/fixtures/big-text-sample.txt";
            let r = tokio::fs::File::open(&fixture)
                .await
                .expect("failed to open text sample file");
            let expected_size = r.metadata().await.unwrap().len() as usize;

            let stream = $fragment_fn(r.compat());

            use futures::TryStreamExt;

            let values: Vec<_> = stream
                .try_collect()
                .await
                .expect("stream should collect without errors");

            assert_eq!(values.len(), $sizes.len());

            let mut sum = 0;

            // This is a cheap way of ensuring the results do not change from one run to the other.
            for (i, (value, expected_len)) in values.iter().zip($sizes).enumerate() {
                let len = value.len();
                sum += len;

                assert_eq!(
                    len, expected_len,
                    "failed to compare chunk #{i}: expected size of {expected_len} but got {len}"
                );
            }

            assert_eq!(expected_size, sum);

            let aggregated = values.concat();
            let expected = tokio::fs::read(fixture)
                .await
                .expect("failed to read text sample file");

            assert_eq!(aggregated, expected);
        };
    }
}
