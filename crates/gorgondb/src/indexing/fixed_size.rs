use std::{marker::PhantomData, mem::size_of, num::NonZeroU64};

use byteorder::ByteOrder;
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::{gorgon::StoreOptions, BlobId, Transaction};

use super::{BinaryTreePath, BinaryTreePathElement, Error};

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
    fn to_bytes(&self) -> Bytes;
}

impl FixedSizeKey for u8 {
    fn to_bytes(&self) -> Bytes {
        vec![*self].into()
    }
}

impl FixedSizeKey for u16 {
    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u16(&mut buf, *self);

        buf.into()
    }
}

impl FixedSizeKey for u32 {
    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u32(&mut buf, *self);

        buf.into()
    }
}

impl FixedSizeKey for u64 {
    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u64(&mut buf, *self);

        buf.into()
    }
}

impl FixedSizeKey for u128 {
    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u128(&mut buf, *self);

        buf.into()
    }
}

type Result<T> = std::result::Result<T, Error<BinaryTreePathElement>>;
type TreeBranch = super::TreeBranch<BinaryTreePathElement, TreeMeta>;
type TreeSearchStack = super::TreeSearchStack<BinaryTreePathElement, TreeMeta>;
type TreeSearchResult = super::TreeSearchResult<BinaryTreePathElement, TreeMeta>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TreeMeta {
    /// The total count of leaf nodes under this node, both directly and indirectly.
    total_count: u64,

    /// The total size of data under this node, not counting the size of intermediate nodes.
    total_size: u64,

    /// The local key size for this node.
    local_key_size: NonZeroU64,
}

/// A distributed map that stores key with a fixed-size.
#[derive(Debug)]
pub struct FixedSizeIndex<'t, Key> {
    _phantom: PhantomData<Key>,
    root_id: BlobId,
    root: TreeBranch,
    transaction: &'t Transaction,
}

impl<'t, Key: FixedSizeKey + Send + Sync> FixedSizeIndex<'t, Key> {
    /// Instantiate a new, empty, index.
    pub async fn initialize(transaction: &'t Transaction) -> Result<Self> {
        // A new tree has no children and a local key size of exactly the full key size,=.
        let root = TreeBranch::new(TreeMeta {
            total_count: 0,
            total_size: 0,
            local_key_size: Key::KEY_SIZE,
        });

        let root_id = transaction
            .store(root.to_vec(), &StoreOptions::default())
            .await?;

        Ok(Self {
            _phantom: Default::default(),
            root,
            root_id,
            transaction,
        })
    }

    /// Load an index from its root id.
    pub async fn load(transaction: &'t Transaction, root_id: BlobId) -> Result<Self> {
        let root = Self::fetch_branch_tx(transaction, &root_id).await?;

        if root.meta.local_key_size > Key::KEY_SIZE {
            return Err(Error::CorruptedTree {
                path: BinaryTreePath::default(),
                err: format!(
                    "root branch has an invalid local key size of {} when at most {} was expected",
                    root.meta.local_key_size,
                    Key::KEY_SIZE
                ),
            });
        }

        Ok(Self {
            _phantom: Default::default(),
            root,
            root_id,
            transaction,
        })
    }

    /// Get the root id for the index.
    ///
    /// This is typically used to persist the tree itself.
    pub fn root_id(&self) -> &BlobId {
        &self.root_id
    }

    /// Turn the index into its root blob id.
    ///
    /// This is typically used to persist the tree itself.
    pub fn into_root_id(self) -> BlobId {
        self.root_id
    }

    /// Get a value from the index.
    pub async fn get(&self, key: &Key) -> Result<Option<BlobId>> {
        Ok(match self.search(key).await? {
            TreeSearchResult::Found { mut stack } => Some(
                stack
                    .top_occupied_entry_mut()
                    .expect("entry should be occupied")
                    .value()
                    .clone(),
            ),
            TreeSearchResult::Missing { .. } => None,
        })
    }

    /// Insert a value in the index.
    ///
    /// If a previous value already existed, it is replaced and returned.
    pub async fn insert(&mut self, key: &Key, mut value: BlobId) -> Result<Option<BlobId>> {
        let new_size = value.size();

        let result;
        let count_diff: u64;
        let size_diff: u64;

        let (stack, mut new_root) = match self.search(key).await? {
            TreeSearchResult::Found { stack } => {
                let (stack, (mut branch, child_key)) = stack.pop_non_empty()?;

                let mut entry = branch.occupied_entry(&child_key);
                let old_value = entry.value();

                if &value == old_value {
                    return Ok(Some(value));
                }

                // Set the parameters for the stack update.
                count_diff = 0;
                size_diff = new_size - old_value.size();

                result = Some(old_value.clone());

                entry.replace(value);
                branch.meta.total_size += size_diff;
                branch.meta.total_count += count_diff;
                value = self.persist_branch(&branch).await?;

                // Note: since the old value is pointing to an externally provided blob id (not a
                // tree node blob id), we don't actually `unstore` it here as it is not our
                // responsibility. After all, we don't `store` the provided `value` either.

                (stack, branch)
            }
            TreeSearchResult::Missing { stack, next_key } => {
                // Set the parameters for the stack update.
                // There was no such key, so no previous value to return.
                count_diff = 1;
                size_diff = new_size;
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
                let (stack, (mut branch, child_key)) = stack.pop_non_empty()?;

                branch.meta.total_size += new_size;
                branch.meta.total_count += count_diff;
                branch.insert_non_existing(child_key, value);

                value = self.persist_branch(&branch).await?;

                (stack, branch)
            }
        };

        // At this point, if we have stack, its top value has a child node at `child_key` that
        // needs to be either inserted or updated with the value in `value`.

        for (mut branch, child_key) in stack {
            let old_value = branch.replace_existing(&child_key, value);

            self.transaction.unstore(&old_value).await?;
            branch.meta.total_size += size_diff;
            branch.meta.total_count += count_diff;

            value = self.persist_branch(&branch).await?;
            new_root = branch;
        }

        self.set_root(value, new_root).await?;

        Ok(result)
    }

    /// Remove a value from the index.
    ///
    /// If the value existed, it is returned.
    pub async fn remove(&mut self, key: &Key) -> Result<Option<BlobId>> {
        Ok(match self.search(key).await? {
            TreeSearchResult::Found { stack } => {
                let (stack, (mut branch, child_key)) = stack.pop_non_empty()?;

                let result = branch.occupied_entry(&child_key).remove();

                branch.meta.total_size -= result.size();
                branch.meta.total_count -= 1;

                let mut value = self.persist_branch(&branch).await?;
                let mut new_root = branch;

                for (mut branch, child_key) in stack {
                    let old_value = branch.replace_existing(&child_key, value);

                    self.transaction.unstore(&old_value).await?;
                    branch.meta.total_size -= result.size();
                    branch.meta.total_count -= 1;

                    value = self.persist_branch(&branch).await?;
                    new_root = branch;
                }

                self.set_root(value, new_root).await?;

                Some(result)
            }
            TreeSearchResult::Missing { .. } => None,
        })
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.root.meta.total_count == 0
    }

    /// Get the count of items in the index.
    ///
    /// This size does not consider intermediate branches, just leaf nodes.
    pub fn len(&self) -> u64 {
        self.root.meta.total_count
    }

    /// Get the total size of the items stored in the index.
    ///
    /// This size does not consider intermediate branches, just leaf nodes.
    pub fn total_size(&self) -> u64 {
        self.root.meta.total_size
    }

    async fn set_root(&mut self, root_id: BlobId, root: TreeBranch) -> Result<()> {
        tracing::trace!("Updating root from {} to {root_id}.", self.root_id);

        self.transaction.unstore(&self.root_id).await?;

        self.root_id = root_id;
        self.root = root;

        Ok(())
    }

    async fn search(&self, key: &Key) -> Result<TreeSearchResult> {
        let key = key.to_bytes();

        let expected_key_size: usize = Key::KEY_SIZE
            .get()
            .try_into()
            .expect("key size should convert to usize");
        assert_eq!(key.len(), expected_key_size);

        let (child_key, next_key) = Self::split_key(key, self.root.meta.local_key_size);
        let stack = TreeSearchStack::new(self.root.clone(), child_key);

        self.search_from(stack, next_key).await
    }

    fn search_from(
        &self,
        mut stack: TreeSearchStack,
        next_key: Option<Bytes>,
    ) -> BoxFuture<'_, Result<TreeSearchResult>> {
        Box::pin(async move {
            match stack.top_occupied_entry_mut() {
                Some(entry) => {
                    match next_key {
                        // If we have no next key, it means we found the value.
                        None => Ok(TreeSearchResult::Found { stack }),
                        Some(next_key) => {
                            let id = entry.value();
                            let branch = self.fetch_branch(id).await?;

                            let (child_key, next_key) =
                                Self::split_key(next_key, branch.meta.local_key_size);
                            stack.push(branch, child_key);

                            self.search_from(stack, next_key).await
                        }
                    }
                }
                None => {
                    // If there is a next key, assume it won't be split and will be used entirely
                    // as a binary tree path element.
                    let next_key = next_key
                        .map(TryInto::try_into)
                        .transpose()
                        .expect("next_key should be constructible from bytes");

                    Ok(super::TreeSearchResult::Missing { stack, next_key })
                }
            }
        })
    }

    async fn fetch_branch_tx(transaction: &Transaction, id: &BlobId) -> Result<TreeBranch> {
        let data = transaction.retrieve_to_memory(id).await?;

        TreeBranch::from_slice(&data).map_err(|err| Error::CorruptedTree {
            path: BinaryTreePath::root(),
            err: err.to_string(),
        })
    }

    async fn fetch_branch(&self, id: &BlobId) -> Result<TreeBranch> {
        Self::fetch_branch_tx(self.transaction, id).await
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

#[cfg(test)]
mod tests {
    use crate::Client;
    use tracing_test::traced_test;

    use super::super::tests::*;
    use super::*;

    #[traced_test]
    #[tokio::test]
    async fn test_fixed_size_index() {
        let client = Client::new_for_tests();
        let tx = client.start_transaction().unwrap();

        assert_transaction_empty!(tx);

        let mut index = FixedSizeIndex::<'_, u16>::initialize(&tx).await.unwrap();

        assert_transaction_empty!(tx);

        let initial_root_id = index.root_id().clone();

        assert_empty!(index);
        assert_total_size!(index, 0);

        assert_key_missing!(index, 1);
        assert_key_missing!(index, 2);
        assert_key_missing!(index, 3);

        assert_insert_new!(index, 1, "one");
        assert_key_exists!(index, 1, "one");
        assert_key_missing!(index, 2);
        assert_key_missing!(index, 3);

        assert_len!(index, 1);
        assert_total_size!(index, 3);

        assert_insert_new!(index, 2, "two");
        assert_key_exists!(index, 1, "one");
        assert_key_exists!(index, 2, "two");
        assert_key_missing!(index, 3);

        assert_len!(index, 2);
        assert_total_size!(index, 6);

        assert_insert_update!(index, 2, "deux", "two");
        assert_key_exists!(index, 1, "one");
        assert_key_exists!(index, 2, "deux");
        assert_key_missing!(index, 3);

        assert_len!(index, 2);
        assert_total_size!(index, 7);

        assert_insert_new!(index, 3, "three");
        assert_key_exists!(index, 1, "one");
        assert_key_exists!(index, 2, "deux");
        assert_key_exists!(index, 3, "three");

        assert_len!(index, 3);
        assert_total_size!(index, 12);

        assert_remove_exists!(index, 1, "one");
        assert_key_missing!(index, 1);
        assert_remove_missing!(index, 1);
        assert_key_exists!(index, 2, "deux");
        assert_key_exists!(index, 3, "three");

        assert_len!(index, 2);
        assert_total_size!(index, 9);

        assert_remove_exists!(index, 2, "deux");
        assert_key_missing!(index, 1);
        assert_key_missing!(index, 2);
        assert_remove_missing!(index, 1);
        assert_remove_missing!(index, 2);
        assert_key_exists!(index, 3, "three");

        assert_len!(index, 1);
        assert_total_size!(index, 5);

        assert_remove_exists!(index, 3, "three");
        assert_key_missing!(index, 1);
        assert_key_missing!(index, 2);
        assert_key_missing!(index, 3);
        assert_remove_missing!(index, 1);
        assert_remove_missing!(index, 2);
        assert_remove_missing!(index, 3);

        assert_empty!(index);
        assert_total_size!(index, 0);

        assert_transaction_empty!(tx);

        let final_root_id = index.root_id().clone();
        assert_eq!(initial_root_id, final_root_id);

        // Insert 1024 values in the tree to trigger actual transaction writing as well as
        // rebalancing.
        for i in 0..1024 {
            assert_insert_new!(
                index,
                i,
                "this is a rather large value that is self-contained"
            );
        }

        assert_transaction_references_count!(tx, 1);

        for i in 0..1024 {
            assert_remove_exists!(
                index,
                i,
                "this is a rather large value that is self-contained"
            );
        }

        assert_empty!(index);
        assert_total_size!(index, 0);

        assert_transaction_empty!(tx);
    }

    #[test]
    fn test_tree_branch_serialization() {
        let tree_branch = TreeBranch::new(TreeMeta {
            total_count: 0,
            total_size: 0,
            local_key_size: NonZeroU64::new(8).unwrap(),
        });

        let buf = tree_branch.to_vec();
        assert_eq!(&[0x92, 0x93, 0x00, 0x00, 0x08, 0x90], buf.as_slice());

        let mut tree_branch = TreeBranch::new(TreeMeta {
            total_count: 2000,
            total_size: 987654321,
            local_key_size: NonZeroU64::new(16).unwrap(),
        });
        tree_branch.insert_non_existing(
            vec![
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                0x0E, 0x0F, 0x10,
            ]
            .into(),
            blob_id!("foo"),
        );

        let buf = tree_branch.to_vec();
        assert_eq!(
            &[
                0x92, 0x93, 0xCD, 0x07, 0xD0, 0xCE, 0x3A, 0xDE, 0x68, 0xB1, 0x10, 0x91, 0x92, 196,
                0x11, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C,
                0x0D, 0x0E, 0x0F, 0x10, 0xA8, 0x41, 0x32, 0x5A, 0x76, 0x62, 0x77, 0x3D, 0x3D
            ],
            buf.as_slice()
        );
    }
}
