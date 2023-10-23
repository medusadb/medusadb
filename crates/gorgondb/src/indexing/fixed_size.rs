use std::{marker::PhantomData, mem::size_of};

use byteorder::ByteOrder;
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{gorgon::StoreOptions, BlobId, Transaction};

use super::{tree::TreeBranchEntry, BinaryTreePath, BinaryTreePathElement};

/// A trait for types that can be used as a fixed-size key.
pub trait FixedSizeKey {
    /// The size of the key.
    const KEY_SIZE: usize;

    /// Convert the value into a slice of bytes.
    fn into_bytes(self) -> Bytes;
}

impl FixedSizeKey for u8 {
    const KEY_SIZE: usize = size_of::<Self>();

    fn into_bytes(self) -> Bytes {
        vec![self].into()
    }
}

impl FixedSizeKey for u16 {
    const KEY_SIZE: usize = size_of::<Self>();

    fn into_bytes(self) -> Bytes {
        let mut buf = vec![0x00; Self::KEY_SIZE];
        byteorder::NetworkEndian::write_u16(&mut buf, self);

        buf.into()
    }
}

impl FixedSizeKey for u32 {
    const KEY_SIZE: usize = size_of::<Self>();

    fn into_bytes(self) -> Bytes {
        let mut buf = vec![0x00; Self::KEY_SIZE];
        byteorder::NetworkEndian::write_u32(&mut buf, self);

        buf.into()
    }
}

impl FixedSizeKey for u64 {
    const KEY_SIZE: usize = size_of::<Self>();

    fn into_bytes(self) -> Bytes {
        let mut buf = vec![0x00; Self::KEY_SIZE];
        byteorder::NetworkEndian::write_u64(&mut buf, self);

        buf.into()
    }
}

impl FixedSizeKey for u128 {
    const KEY_SIZE: usize = size_of::<Self>();

    fn into_bytes(self) -> Bytes {
        let mut buf = vec![0x00; Self::KEY_SIZE];
        byteorder::NetworkEndian::write_u128(&mut buf, self);

        buf.into()
    }
}

/// A distributed map that stores key with a fixed-size.
#[derive(Debug)]
pub struct FixedSizeIndex<'t, Key> {
    _phantom: PhantomData<Key>,
    root: BlobId,
    transaction: &'t Transaction,
}

/// An error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error happened at the transaction level.
    #[error("transaction: {0}")]
    TransactionError(#[from] crate::gorgon::Error),

    /// The tree is corrupted.
    #[error("tree is corrupted at `{path}`: {err}")]
    CorruptedTree { path: BinaryTreePath, err: String },
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default, Serialize, Deserialize)]
struct TreeMeta {
    total_count: usize,
    local_key_size: usize,
}

type TreeBranch = super::TreeBranch<TreeMeta>;
type TreeSearchResult = super::TreeSearchResult<BinaryTreePathElement, TreeMeta>;

impl<'t, Key: FixedSizeKey + Send + Sync> FixedSizeIndex<'t, Key> {
    /// Instantiate a new, empty, index.
    pub async fn initialize(transaction: &'t Transaction) -> Result<FixedSizeIndex<'t, Key>> {
        let root_data = TreeBranch::default().to_vec();
        let root = transaction
            .store(root_data, &StoreOptions::default())
            .await?;

        Ok(Self {
            _phantom: Default::default(),
            root,
            transaction,
        })
    }

    /// Insert a value in the index.
    ///
    /// If a previous value already existed, it is replaced and returned.
    pub async fn insert(&self, key: Key, value: &BlobId) -> Result<Option<BlobId>> {
        let key = key.into_bytes();

        match self.search(key).await? {
            TreeSearchResult::Found {
                path,
                mut parent,
                id,
            } => {
                // If the value being inserted is the same than the one already present, avoid the
                // useless write.
                if value == &id {
                    debug!("Found identical existing entry in fixed-sized index at `{path}` (`{id}`): skipping update.");

                    return Ok(Some(id));
                }

                debug!("Found existing entry in fixed-sized index at `{path}` (`{id}`): replacing with `{value}`");

                parent.entry(key)
            }
            TreeSearchResult::Missing {
                parent_path,
                parent,
                local_key,
                next_key,
            } => todo!(),
        }
    }

    async fn search(&self, key: Bytes) -> Result<TreeSearchResult> {
        let root = self.get_root().await?;
        let path = BinaryTreePath::default();

        self.search_from(path, root, key).await
    }

    fn search_from(
        &self,
        mut parent_path: BinaryTreePath,
        mut node: TreeBranch,
        key: Bytes,
    ) -> BoxFuture<'_, Result<TreeSearchResult>> {
        Box::pin(async move {
            // If the tree node has no local key size, we exit early.
            if node.meta.local_key_size == 0 {
                return Ok(TreeSearchResult::Missing {
                    parent_path,
                    parent: node,
                    local_key: key.into(),
                    next_key: Bytes::default().into(),
                });
            }

            let (local_key, next_key) = Self::split_key(key, node.meta.local_key_size);

            match node.entry(local_key) {
                TreeBranchEntry::Occupied(entry) => {
                    // If we have no next key, it means we found the value.
                    if next_key.is_empty() {
                        let id = entry.get().clone();
                        parent_path.push(entry.into_key());

                        Ok(TreeSearchResult::Found {
                            path: parent_path,
                            parent: node,
                            id,
                        })
                    } else {
                        // We can remove the values from the local node to avoid clones as we we
                        // will fetch the new parent node anyway.
                        let (local_key, id) = entry.remove();
                        parent_path.push(local_key);

                        let node = self.get_branch(&id).await?;

                        self.search_from(parent_path, node, next_key).await
                    }
                }
                TreeBranchEntry::Vacant(entry) => {
                    let local_key = entry.into_key().into();
                    let next_key = next_key.into();

                    Ok(super::TreeSearchResult::Missing {
                        parent_path,
                        parent: node,
                        local_key,
                        next_key,
                    })
                }
            }
        })
    }

    async fn get_root(&self) -> Result<TreeBranch> {
        self.get_branch(&self.root).await
    }

    async fn get_branch(&self, id: &BlobId) -> Result<TreeBranch> {
        let data = self.transaction.retrieve_to_memory(id).await?;

        TreeBranch::from_slice(&data).map_err(|err| Error::CorruptedTree {
            path: BinaryTreePath::root(),
            err: err.to_string(),
        })
    }

    fn split_key(mut key: Bytes, key_size: usize) -> (Bytes, Bytes) {
        assert!(key_size > 0);

        let next_key = key.split_off(key_size);

        (key, next_key)
    }
}
