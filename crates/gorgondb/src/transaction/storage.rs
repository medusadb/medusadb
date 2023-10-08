use std::{borrow::Cow, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::RwLock;

use crate::{
    gorgon::{Retrieve, Store, Unstore},
    storage::{Error, FilesystemStorage},
    Filesystem, RemoteRef,
};

// An in-memory/on-disk local storage for transactions.
#[derive(Debug)]
pub(crate) struct Storage {
    disk_size_threshold: u64,
    blobs: Arc<RwLock<HashMap<RemoteRef, RefCountedBlob>>>,
    filesystem_storage: FilesystemStorage,
    _filesystem_root: TempDir, // Keep the folder alive.
    base_storage: Arc<crate::Storage>,
}

impl Storage {
    pub(crate) fn new(
        disk_size_threshold: u64,
        filesystem: Filesystem,
        base_storage: Arc<crate::Storage>,
    ) -> std::io::Result<Self> {
        let _filesystem_root = tempfile::TempDir::new()?;
        let filesystem_storage = FilesystemStorage::new(filesystem, _filesystem_root.path())?;

        Ok(Self {
            disk_size_threshold,
            blobs: Arc::default(),
            filesystem_storage,
            _filesystem_root,
            base_storage,
        })
    }

    pub(crate) async fn commit(self) -> crate::storage::Result<(), (Self, crate::storage::Error)> {
        let blobs = Arc::into_inner(self.blobs)
            .expect("there should be only one active reference to this transaction storage")
            .into_inner();
        let mut blobs_iter = blobs.into_iter();

        while let Some((remote_ref, ref_counted_blob)) = blobs_iter.next() {
            if let Err(err) = match &ref_counted_blob.data {
                Some(data) =>self.base_storage.store(&remote_ref, data.clone().into()).await,
                None => match self.filesystem_storage.retrieve(&remote_ref).await {
                    Ok(Some(source)) => {
                        self.base_storage.store(&remote_ref, source.into()).await
                    }
                    Ok(None) => Err(Error::DataCorruption(format!("the remote-ref `{remote_ref}` should exist in the local filesystem storage"))),
                    Err(err) => Err(err.into()),
                },
            } {
                // Make sure we add back to to blobs the non-handled ones.
                return Err((Self{
                    disk_size_threshold: self.disk_size_threshold,
                    blobs: Arc::new(RwLock::new(std::iter::once((remote_ref, ref_counted_blob)).chain(blobs_iter).collect())),
                    filesystem_storage: self.filesystem_storage,
                    _filesystem_root: self._filesystem_root,
                    base_storage: self.base_storage,
                }, err))
            }
        }

        Ok(())
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
                let data = if source.size() > self.disk_size_threshold {
                    self.filesystem_storage.store(entry.key(), source).await?;

                    None
                } else {
                    Some(source.read_all_into_vec().await?.into())
                };

                entry.insert(RefCountedBlob::new(data));
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
    ) -> crate::storage::Result<Option<crate::AsyncSource<'s>>> {
        let blobs = self.blobs.read().await;

        match blobs.get(remote_ref) {
            Some(ref_blob) => match &ref_blob.data {
                Some(data) => Ok(Some(data.clone().into())),
                None => self
                    .filesystem_storage
                    .retrieve(remote_ref)
                    .await
                    .map(|o| o.map(Into::into))
                    .map_err(Into::into),
            },
            None => {
                // Make sure we free the read-lock before reading from the base storage.
                drop(blobs);

                // The reference was not found: check in the base-storage.
                self.base_storage.retrieve(remote_ref).await
            }
        }
    }
}

#[async_trait]
impl Unstore for Storage {
    async fn unstore(&self, remote_ref: &RemoteRef) {
        if let std::collections::hash_map::Entry::Occupied(entry) =
            self.blobs.write().await.entry(remote_ref.clone())
        {
            // The entry exists already: if it has a single reference, we must delete it
            // entirely.
            if entry.get().count == 0 {
                entry.remove();
            } else {
                entry.into_mut().count -= 1;
            }
        }
    }
}

#[derive(Debug, Default)]
struct RefCountedBlob {
    count: u32,
    data: Option<Cow<'static, [u8]>>,
}

impl RefCountedBlob {
    fn new(data: Option<Cow<'static, [u8]>>) -> Self {
        Self { data, count: 0 }
    }
}
