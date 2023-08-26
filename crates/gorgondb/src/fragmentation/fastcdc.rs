use bytes::Bytes;
use futures::{AsyncRead, Stream, StreamExt, TryStream};
use pin_project::pin_project;

use super::Error;

/// Parameters for the `FastCDC` rolling-hash algorithm.
#[derive(Debug, Clone)]
pub struct Fastcdc {
    min_size: u32,
    avg_size: u32,
    max_size: u32,
}

impl Default for Fastcdc {
    fn default() -> Self {
        Self::new(2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024)
    }
}

impl Fastcdc {
    /// Instantiate a new `FastCDC` parameters structure.
    ///
    /// The `min_size` must be strictly smaller than the average size, which must be strictly
    /// smaller than the maximum size.
    pub fn new(min_size: u32, avg_size: u32, max_size: u32) -> Self {
        assert!(
            (fastcdc::v2020::MINIMUM_MIN..=fastcdc::v2020::MINIMUM_MAX).contains(&min_size),
            "min_size must be between {} and {}",
            fastcdc::v2020::MINIMUM_MIN,
            fastcdc::v2020::MINIMUM_MAX,
        );
        assert!(
            (fastcdc::v2020::AVERAGE_MIN..=fastcdc::v2020::AVERAGE_MAX).contains(&avg_size),
            "avg_size must be between {} and {}",
            fastcdc::v2020::AVERAGE_MIN,
            fastcdc::v2020::AVERAGE_MAX,
        );
        assert!(
            (fastcdc::v2020::MAXIMUM_MIN..=fastcdc::v2020::MAXIMUM_MAX).contains(&max_size),
            "max_size must be between {} and {}",
            fastcdc::v2020::MAXIMUM_MIN,
            fastcdc::v2020::MAXIMUM_MAX,
        );

        assert!(
            min_size < avg_size,
            "`min_size` must be strictly inferior to `avg_size`"
        );
        assert!(
            avg_size < max_size,
            "`avg_size` must be strictly inferior to `max_size`"
        );

        Self {
            min_size,
            avg_size,
            max_size,
        }
    }

    /// Fragment the specified source buffer asynchronously.
    pub fn fragment<'r>(
        &self,
        r: impl AsyncRead + Unpin + 'r,
    ) -> impl TryStream<Ok = Bytes, Error = super::Error> + 'r {
        Fragmenter::new(r, self.min_size, self.avg_size, self.max_size)
    }
}

#[pin_project]
struct Fragmenter<R> {
    stream: fastcdc::v2020::AsyncStreamCDC<R>,
}

impl<R: AsyncRead + Unpin> Fragmenter<R> {
    fn new(r: R, min_size: u32, avg_size: u32, max_size: u32) -> Self {
        let stream = fastcdc::v2020::AsyncStreamCDC::new(r, min_size, avg_size, max_size);

        Self { stream }
    }
}

impl<R: AsyncRead + Unpin> Stream for Fragmenter<R> {
    type Item = Result<Bytes, Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let mut stream = Box::pin(this.stream.as_stream());

        stream
            .poll_next_unpin(cx)
            .map_ok(|chunk| chunk.data.into())
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use async_compat::CompatExt;

    use crate::test_fragmentation;

    use super::*;

    #[tokio::test]
    async fn test_fragment() {
        let f = Fastcdc::new(64, 256, 1024);
        let sizes = [350, 427, 289, 456, 298, 520, 433, 300, 364, 104];

        test_fragmentation!(|r| f.fragment(r), sizes);
    }
}
