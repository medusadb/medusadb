use futures::AsyncRead;

/// An `AsyncRead` that returns a failure inconditionally.
#[derive(Debug, Default)]
pub struct AsyncReadFailure;

impl AsyncRead for AsyncReadFailure {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "item was not found".to_owned(),
        )))
    }
}
