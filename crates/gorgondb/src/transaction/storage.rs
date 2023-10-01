use std::{borrow::Cow, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::RwLock;

use crate::{
    gorgon::{Retrieve, Store},
    storage::FilesystemStorage,
    Filesystem, RemoteRef,
};

// An in-memory/on-disk local storage for transactions.
#[derive(Debug)]
pub(crate) struct Storage<'d>(Arc<RwLock<StorageImpl<'d>>>);

impl Storage<'_> {
    const DISK_THRESHOLD: u64 = 1024 * 1024;

    pub(crate) fn new(filesystem: Filesystem) -> std::io::Result<Self> {
        StorageImpl::new(Self::DISK_THRESHOLD, filesystem)
            .map(RwLock::new)
            .map(Arc::new)
            .map(Self)
    }
}

#[async_trait]
impl<'d> Store for Storage<'d> {
    async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: crate::AsyncSource<'_>,
    ) -> crate::storage::Result<()> {
        self.0.write().await.store(remote_ref.clone(), source).await
    }
}

#[async_trait]
impl<'d> Retrieve<'d> for Storage<'d> {
    async fn retrieve(
        &'d self,
        remote_ref: &RemoteRef,
    ) -> crate::storage::Result<crate::AsyncSource<'d>> {
        self.0
            .read()
            .await
            .retrieve(remote_ref)
            .await
            .map_err(Into::into)
    }
}

#[derive(Debug)]
struct StorageImpl<'d> {
    disk_threshold: u64,
    blobs: HashMap<RemoteRef, RefCountedBlob<'d>>,
    filesystem_storage: FilesystemStorage,
    _filesystem_root: TempDir, // Keep the folder alive.
}

impl<'d> StorageImpl<'d> {
    fn new(disk_threshold: u64, filesystem: Filesystem) -> std::io::Result<Self> {
        let filesystem_root = tempfile::TempDir::new()?;
        let filesystem_storage = FilesystemStorage::new(filesystem, filesystem_root.path())?;

        Ok(Self {
            disk_threshold,
            blobs: HashMap::default(),
            filesystem_storage,
            _filesystem_root: filesystem_root,
        })
    }

    async fn retrieve(&self, remote_ref: &RemoteRef) -> std::io::Result<crate::AsyncSource<'d>> {
        let ref_blob = self.blobs.get(remote_ref).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no remote-ref was found for `{remote_ref}`"),
            )
        })?;

        match &ref_blob.data {
            Some(data) => Ok(data.clone().into()),
            None => self
                .filesystem_storage
                .retrieve(remote_ref)
                .await
                .map(Into::into),
        }
    }

    async fn store(
        &mut self,
        remote_ref: RemoteRef,
        source: crate::AsyncSource<'_>,
    ) -> crate::storage::Result<()> {
        match self.blobs.entry(remote_ref) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                // The entry exists already: just increment the reference count.
                entry.into_mut().count += 1;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let data = if source.size() > self.disk_threshold {
                    self.filesystem_storage.store(entry.key(), source).await?;

                    None
                } else {
                    Some(source.read_all_into_vec().await?.into())
                };

                entry.insert(RefCountedBlob { count: 1, data });
            }
        };

        Ok(())
    }
}

#[derive(Debug, Default)]
struct RefCountedBlob<'d> {
    count: u32,
    data: Option<Cow<'d, [u8]>>,
}
