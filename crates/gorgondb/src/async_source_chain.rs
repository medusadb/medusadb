use std::sync::Arc;

use futures::AsyncReadExt;

use crate::{AsyncSource, BoxAsyncRead};

/// A chain of async sources.
#[derive(Debug, Clone)]
pub struct AsyncSourceChain<'d> {
    sources: Arc<Vec<AsyncSource<'d>>>,
    size: u64,
}

impl<'d> AsyncSourceChain<'d> {
    /// Instantiate a new chain of `AsyncSource`.
    pub fn new(sources: Vec<AsyncSource<'d>>) -> Self {
        let size = sources.iter().map(|s| s.size()).sum();
        let sources = Arc::new(sources);

        Self { sources, size }
    }

    /// Get the size of the referenced data.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Read all the contained `AsyncSource` into a buffer in memory.
    pub async fn read_all_into_memory(self) -> std::io::Result<Vec<u8>> {
        let data_size: usize = self
            .size
            .try_into()
            .expect("failed to convert u64 to usize");
        let mut data = Vec::with_capacity(data_size);

        for s in self.sources.iter() {
            let mut r = s.get_async_read().await?;
            r.read_to_end(&mut data).await?;
        }

        debug_assert_eq!(data.len(), data_size);

        Ok(data)
    }

    /// Get an async reader returning the referenced data.
    pub async fn get_async_read(&self) -> std::io::Result<BoxAsyncRead> {
        let streams = futures::future::join_all(self.sources.iter().map(|s| s.get_async_read()))
            .await
            .into_iter()
            .collect::<std::io::Result<Vec<_>>>()?;

        Ok(streams
            .into_iter()
            .reduce(|res, stream| Box::new(res.chain(stream)))
            .unwrap_or_else(|| Box::new(futures::io::empty())))
    }
}
