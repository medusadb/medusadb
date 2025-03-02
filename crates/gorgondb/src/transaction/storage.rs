use std::{borrow::Cow, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, trace};

use crate::{
    Filesystem, RemoteRef,
    gorgon::{Retrieve, Store, Unstore},
    storage::{Error, FilesystemStorage},
};

// An in-memory/on-disk local storage for transactions.
#[derive(Debug)]
pub(crate) struct Storage {
    name: Cow<'static, str>,
    disk_size_threshold: u64,
    remote_refs: RwLock<HashMap<RemoteRef, RefCountedBlob>>,
    filesystem_storage: FilesystemStorage,
    base_storage: Arc<crate::Storage>,
}

impl Storage {
    pub(crate) fn new(
        name: impl Into<Cow<'static, str>>,
        disk_size_threshold: u64,
        filesystem: Filesystem,
        base_storage: Arc<crate::Storage>,
    ) -> std::io::Result<Self> {
        let filesystem_storage = FilesystemStorage::new_temporary(filesystem)?;
        let name = name.into();

        Ok(Self {
            name,
            disk_size_threshold,
            remote_refs: Default::default(),
            filesystem_storage,
            base_storage,
        })
    }

    /// Get the name of the transaction storage.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Fork the storage, duplicating its content in memory.
    ///
    /// The resulting transaction is exactly identical to the initial one, but the two are
    /// independant after the fork.
    pub async fn fork(&self, name: impl Into<Cow<'static, str>>) -> Self {
        let name = name.into();
        let remote_refs = self.remote_refs.read().await.clone();

        debug!(
            "Forked transaction storage `{}` to `{name}` with {} remote ref(s).",
            self.name,
            remote_refs.len()
        );

        Self {
            name,
            disk_size_threshold: self.disk_size_threshold,
            remote_refs: RwLock::new(remote_refs),
            filesystem_storage: self.filesystem_storage.clone(),
            base_storage: self.base_storage.clone(),
        }
    }

    /// Get the reference remote refs.
    pub(crate) async fn get_remote_refs(&self) -> Vec<RemoteRef> {
        self.remote_refs.read().await.keys().cloned().collect()
    }

    pub(crate) async fn commit(self) -> crate::storage::Result<(), (Self, crate::storage::Error)> {
        let blobs = self.remote_refs.into_inner();
        let mut blobs_iter = blobs.into_iter();

        while let Some((remote_ref, ref_counted_blob)) = blobs_iter.next() {
            if let Err(err) = match &ref_counted_blob.data {
                Some(data) => {
                    self.base_storage
                        .store(&remote_ref, data.clone().into())
                        .await
                }
                None => match self.filesystem_storage.retrieve(&remote_ref).await {
                    Ok(Some(source)) => self.base_storage.store(&remote_ref, source.into()).await,
                    Ok(None) => Err(Error::DataCorruption(format!(
                        "the remote-ref `{remote_ref}` should exist in the local filesystem storage"
                    ))),
                    Err(err) => Err(err.into()),
                },
            } {
                // Make sure we add back to to blobs the non-handled ones.
                return Err((
                    Self {
                        name: self.name,
                        disk_size_threshold: self.disk_size_threshold,
                        remote_refs: RwLock::new(
                            std::iter::once((remote_ref, ref_counted_blob))
                                .chain(blobs_iter)
                                .collect(),
                        ),
                        filesystem_storage: self.filesystem_storage,
                        base_storage: self.base_storage,
                    },
                    err,
                ));
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
        match self.remote_refs.write().await.entry(remote_ref.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                // The entry exists already: just increment the reference count.
                let entry = entry.into_mut();

                entry.count += 1;

                trace!(
                    "Incremented reference count of `{remote_ref}` to {} in transaction storage `{}`",
                    entry.count, self.name
                );
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let data = if source.size() > self.disk_size_threshold {
                    self.filesystem_storage.store(entry.key(), source).await?;

                    None
                } else {
                    Some(source.read_all_into_owned_memory().await?)
                };

                entry.insert(RefCountedBlob::new(data));

                trace!(
                    "Added new reference to `{remote_ref}` in transaction storage `{}`",
                    self.name
                );
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
        let blobs = self.remote_refs.read().await;

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
        match self.remote_refs.write().await.entry(remote_ref.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                // The entry exists already: if it has a single reference, we must delete it
                // entirely.
                if entry.get().is_last_ref() {
                    entry.remove();

                    trace!(
                        "Removed last reference to `{remote_ref}` from transaction storage `{}`",
                        self.name
                    );
                } else {
                    let entry = entry.into_mut();

                    entry.count -= 1;

                    trace!(
                        "Decremented reference count to `{remote_ref}` to {} in transaction storage `{}`",
                        entry.count, self.name
                    );
                }
            }
            _ => {
                trace!(
                    "No existing reference to `{remote_ref}` in transaction storage `{}`: nothing to unstore.",
                    self.name
                );
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RefCountedBlob {
    count: u32,
    data: Option<Cow<'static, [u8]>>,
}

impl RefCountedBlob {
    fn new(data: Option<Cow<'static, [u8]>>) -> Self {
        Self { data, count: 0 }
    }

    fn is_last_ref(&self) -> bool {
        self.count == 0
    }
}
