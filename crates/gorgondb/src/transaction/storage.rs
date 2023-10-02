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
pub(crate) struct Storage {
    disk_threshold: u64,
    blobs: Arc<RwLock<HashMap<RemoteRef, RefCountedBlob>>>,
    filesystem_storage: FilesystemStorage,
    _filesystem_root: TempDir, // Keep the folder alive.
}

impl Storage {
    pub(crate) fn new(disk_threshold: u64, filesystem: Filesystem) -> std::io::Result<Self> {
        let filesystem_root = tempfile::TempDir::new()?;
        let filesystem_storage = FilesystemStorage::new(filesystem, filesystem_root.path())?;

        Ok(Self {
            disk_threshold,
            blobs: Arc::default(),
            filesystem_storage,
            _filesystem_root: filesystem_root,
        })
    }
}

#[async_trait]
impl Store for Storage {
    async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: crate::AsyncSource<'_>,
    ) -> crate::storage::Result<()> {
        match self.blobs.write().await.entry(remote_ref.clone()) {
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

#[async_trait]
impl Retrieve for Storage {
    async fn retrieve<'s>(
        &'s self,
        remote_ref: &RemoteRef,
    ) -> crate::storage::Result<crate::AsyncSource<'s>> {
        let blobs = self.blobs.read().await;

        let ref_blob = blobs.get(remote_ref).ok_or_else(|| {
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
                .map(Into::into)
                .map_err(Into::into),
        }
    }
}

#[derive(Debug, Default)]
struct RefCountedBlob {
    count: u32,
    data: Option<Cow<'static, [u8]>>,
}
