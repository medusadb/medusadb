use std::{marker::PhantomData, mem::size_of, num::NonZeroU64};

use byteorder::ByteOrder;
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::{gorgon::StoreOptions, BlobId, Transaction};

use super::{tree::TreeBranchEntry, BinaryTreePath, BinaryTreePathElement};

/// A trait for types that can be used as a fixed-size key.
pub trait FixedSizeKey: Sized + Ord + Eq {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(size_of::<Self>() as u64) };

    /// Make a zero-buffer of the appropriate size to store a serialized version of keys of this
    /// type.
    fn make_buf() -> Vec<u8> {
        vec![
            0x00;
            Self::KEY_SIZE
                .get()
                .try_into()
                .expect("could not convert key size to usize")
        ]
    }

    /// Convert the value into a slice of bytes.
    fn into_bytes(self) -> Bytes;
}

impl FixedSizeKey for u8 {
    fn into_bytes(self) -> Bytes {
        vec![self].into()
    }
}

impl FixedSizeKey for u16 {
    fn into_bytes(self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u16(&mut buf, self);

        buf.into()
    }
}

impl FixedSizeKey for u32 {
    fn into_bytes(self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u32(&mut buf, self);

        buf.into()
    }
}

impl FixedSizeKey for u64 {
    fn into_bytes(self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u64(&mut buf, self);

        buf.into()
    }
}

impl FixedSizeKey for u128 {
    fn into_bytes(self) -> Bytes {
        let mut buf = Self::make_buf();
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

#[derive(Debug, Serialize, Deserialize)]
struct TreeMeta {
    /// The total count of leaf nodes under this node, both directly and indirectly.
    total_count: u64,

    /// The total size of data under this node, not counting the size of intermediate nodes.
    total_size: u64,

    /// The local key size for this node.
    local_key_size: NonZeroU64,
}

type TreeBranch = super::TreeBranch<BinaryTreePathElement, TreeMeta>;
type TreeSearchStack = super::TreeSearchStack<BinaryTreePathElement, TreeMeta>;
type TreeSearchResult = super::TreeSearchResult<BinaryTreePathElement, TreeMeta>;

#[derive(Debug, Clone, Copy)]
enum SizeUpdate {
    Increment(u64),
    Decrement(u64),
}

impl SizeUpdate {
    fn apply_to(self, value: NonZeroU64) -> NonZeroU64 {
        match self {
            SizeUpdate::Increment(i) => value.checked_add(i).expect("invalid size update"),
            SizeUpdate::Decrement(i) => {
                NonZeroU64::new(value.get().checked_sub(i).expect("overflow"))
                    .expect("invalid size update")
            }
        }
    }
}

impl<'t, Key: FixedSizeKey + Send + Sync> FixedSizeIndex<'t, Key> {
    /// Instantiate a new, empty, index.
    pub async fn initialize(transaction: &'t Transaction) -> Result<FixedSizeIndex<'t, Key>> {
        // A new tree has no children and a local key size of exactly the full key size,=.
        let root_data = TreeBranch::new(TreeMeta {
            total_count: 0,
            total_size: 0,
            local_key_size: Key::KEY_SIZE,
        })
        .to_vec();

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
    pub async fn insert(&mut self, key: Key, mut value: BlobId) -> Result<Option<BlobId>> {
        let new_size = value.size();

        let result;
        let count_diff: u64;
        let size_diff: SizeUpdate;

        let (mut stack, mut child_key) = match self.search(key).await? {
            TreeSearchResult::Found { stack, found_key } => {
                // If the value being inserted is the same than the one already present, avoid the
                // useless write.
                let branch = stack.top();
                let old_value = branch.get_existing(&found_key);

                if &value == old_value {
                    return Ok(Some(value));
                }

                // Set the parameters for the stack update.
                count_diff = 0;
                let old_size = old_value.size();

                if new_size > old_size {
                    size_diff = SizeUpdate::Increment(new_size - old_size);
                } else {
                    size_diff = SizeUpdate::Decrement(new_size - old_size);
                }

                result = Some(old_value.clone());

                // Note: since the old value is pointing to an externally provided blob id (not a
                // tree node blob id), we don't actually `unstore` it here as it is not our
                // responsibility. After all, we don't `store` the provided `value` either.

                (Some(stack), found_key)
            }
            TreeSearchResult::Missing {
                stack,
                missing_key,
                next_key,
            } => {
                // Set the parameters for the stack update.
                // There was no such key, so no previous value to return.
                count_diff = 1;
                size_diff = SizeUpdate::Increment(new_size);
                result = None;

                // There is some more key material to create under the missing key:
                // start with this.
                if let Some(next_key) = next_key {
                    let meta = TreeMeta {
                        total_size: new_size,
                        total_count: count_diff,
                        local_key_size: next_key.size(),
                    };

                    let branch = TreeBranch::new_with_single_child(meta, next_key, value);

                    value = self.persist_branch(&branch).await?;
                }

                // If we were to return the stack as-is, its top element would NOT have the
                // associated key, which is not what we want. So we pop it here and deal with
                // it as a special case so that the rest of the stack unwinding can expect the
                // child to always be there.
                let (stack, (next_key, mut branch)) = stack.pop();
                branch.meta.local_key_size = branch
                    .meta
                    .local_key_size
                    .checked_add(new_size)
                    .expect("unexpected overflow");
                branch.meta.total_count += count_diff;
                branch.insert_non_existing(missing_key, value);

                value = self.persist_branch(&branch).await?;

                (stack, next_key)
            }
        };

        // At this point, if we have stack, its top value has a child node at `child_key` that
        // needs to be either inserted or updated with the value in `value`.

        while let Some(new_stack) = stack {
            let (next_key, mut branch);
            (stack, (next_key, branch)) = new_stack.pop();
            let old_value = branch.replace_existing(child_key, value);

            self.transaction.unstore(&old_value).await?;
            branch.meta.local_key_size = size_diff.apply_to(branch.meta.local_key_size);
            branch.meta.total_count += count_diff;

            child_key = next_key;

            value = self.persist_branch(&branch).await?;
        }

        self.root = value;
        Ok(result)
    }

    async fn search(&self, key: Key) -> Result<TreeSearchResult> {
        let key = key.into_bytes();

        let expected_key_size: usize = Key::KEY_SIZE
            .get()
            .try_into()
            .expect("key size should convert to usize");
        assert_eq!(key.len(), expected_key_size);

        let root = self.get_root().await?;
        let stack = TreeSearchStack::new(root);

        self.search_from(stack, key).await
    }

    fn search_from(
        &self,
        mut stack: TreeSearchStack,
        key: Bytes,
    ) -> BoxFuture<'_, Result<TreeSearchResult>> {
        Box::pin(async move {
            let top = stack.top_mut();
            let local_key_size = top.meta.local_key_size;

            let (local_key, next_key) = Self::split_key(key, local_key_size);

            match top.entry(local_key) {
                TreeBranchEntry::Occupied(entry) => {
                    match next_key {
                        // If we have no next key, it means we found the value.
                        None => {
                            let found_key = entry.into_key_elem();

                            Ok(TreeSearchResult::Found { stack, found_key })
                        }
                        Some(next_key) => {
                            // We can remove the values from the local node to avoid clones as we we
                            // will fetch the new parent node anyway.
                            let (local_key, id) = entry.remove();
                            let node = self.fetch_branch(&id).await?;

                            stack.push(local_key, node);

                            self.search_from(stack, next_key).await
                        }
                    }
                }
                TreeBranchEntry::Vacant(entry) => {
                    // If there is a next key, assume it won't be split and will be used entirely
                    // as a binary tree path element.
                    let next_key = next_key
                        .map(TryInto::try_into)
                        .transpose()
                        .expect("next_key should be constructible from bytes");

                    let missing_key = entry.into_key_elem();

                    Ok(super::TreeSearchResult::Missing {
                        stack,
                        missing_key,
                        next_key,
                    })
                }
            }
        })
    }

    async fn get_root(&self) -> Result<TreeBranch> {
        self.fetch_branch(&self.root).await
    }

    async fn fetch_branch(&self, id: &BlobId) -> Result<TreeBranch> {
        let data = self.transaction.retrieve_to_memory(id).await?;

        TreeBranch::from_slice(&data).map_err(|err| Error::CorruptedTree {
            path: BinaryTreePath::root(),
            err: err.to_string(),
        })
    }

    async fn persist_branch(&self, branch: &TreeBranch) -> Result<BlobId> {
        let data = branch.to_vec();

        self.transaction
            .store(data, &StoreOptions::default())
            .await
            .map_err(Into::into)
    }

    /// Split the specified key at the specified size.
    ///
    /// If there is no key material left after the split, `None` is returned.
    fn split_key(mut key: Bytes, key_size: NonZeroU64) -> (BinaryTreePathElement, Option<Bytes>) {
        let next_key = key.split_off(
            key_size
                .get()
                .try_into()
                .expect("key size should convert to usize"),
        );

        (
            key.try_into()
                .expect("key should convert to tree path element"),
            if next_key.is_empty() {
                None
            } else {
                Some(next_key)
            },
        )
    }
}
