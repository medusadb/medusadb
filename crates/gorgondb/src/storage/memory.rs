//! A storage that keeps everything in memory.

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{AsyncSource, RemoteRef};

/// A storage that stores value on disk.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    blobs: Arc<Mutex<HashMap<RemoteRef, Cow<'static, [u8]>>>>,
}

impl MemoryStorage {
    /// Retrieve a value on disk.
    pub fn retrieve(&self, remote_ref: &RemoteRef) -> Option<AsyncSource<'static>> {
        self.blobs
            .lock()
            .unwrap()
            .get(remote_ref)
            .map(|v| v.clone().into())
    }

    /// Store a value on disk and retrieve it right away.
    ///
    /// If the file already exists, it is assumed to exist and contain the expected value. As such,
    /// the function will return immediately, in success.
    pub async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: impl Into<AsyncSource<'_>>,
    ) -> std::io::Result<()> {
        let source = source.into();

        let data = source.read_all_into_owned_memory().await?;

        self.blobs.lock().unwrap().insert(remote_ref.clone(), data);

        Ok(())
    }
}
