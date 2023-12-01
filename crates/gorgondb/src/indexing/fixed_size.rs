use std::{
    marker::PhantomData,
    mem::size_of,
    num::{NonZeroU64, NonZeroUsize},
};

use byteorder::ByteOrder;
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::{gorgon::StoreOptions, BlobId, Transaction};

use super::{
    tree::{ByteDeserialize, ByteSerialize, TreeItem},
    BinaryTreePath, BinaryTreePathElement, Cache, Error,
};

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
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(size_of::<Self>() as u64) };

    fn to_bytes(&self) -> Bytes {
        vec![*self].into()
    }
}

impl FixedSizeKey for u16 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(size_of::<Self>() as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u16(&mut buf, *self);

        buf.into()
    }
}

impl FixedSizeKey for u32 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(size_of::<Self>() as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u32(&mut buf, *self);

        buf.into()
    }
}

impl FixedSizeKey for u64 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(size_of::<Self>() as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u64(&mut buf, *self);

        buf.into()
    }
}

impl FixedSizeKey for u128 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(size_of::<Self>() as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u128(&mut buf, *self);

        buf.into()
    }
}

type Result<T, E = Error<BinaryTreePathElement>> = std::result::Result<T, E>;
type TreeBranch = super::TreeBranch<BinaryTreePathElement, TreeMeta>;
type TreeSearchStack = super::TreeSearchStack<BinaryTreePathElement, TreeMeta>;
type TreeSearchResult = super::TreeSearchResult<BinaryTreePathElement, TreeMeta>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TreeMeta {
    /// The total count of leaf nodes under this node, both directly and indirectly.
    total_count: u64,

    /// The total size of data under this node, not counting the size of intermediate nodes.
    total_size: u64,
}

impl TreeMeta {
    pub(crate) fn adjust_total_size<E: std::fmt::Debug>(
        &mut self,
        diff: impl TryInto<i128, Error = E>,
    ) {
        let total_size: i128 = self.total_size.try_into().expect("should fit into a i128");
        let total_size = total_size + diff.try_into().expect("should fit into a i128");
        self.total_size = total_size.try_into().expect("should fit into a i64");
    }
}

impl TreeBranch {
    fn local_key_size(&self) -> Option<NonZeroU64> {
        self.children.first().and_then(|TreeItem(key, _)| {
            NonZeroU64::new(key.0.len().try_into().expect("should convert to u64"))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TreeRoot {
    branch_id: Option<BlobId>,
    min_count: u64,
    max_count: u64,
}

impl TreeRoot {
    fn new_for_max_tree_size(max_tree_size: u64, key_size: u64) -> Self {
        let max_count = max_tree_size / (key_size + 64);
        let min_count = max_count / 2;

        Self {
            branch_id: None,
            min_count,
            max_count,
        }
    }
}

impl ByteSerialize for TreeRoot {}
impl ByteDeserialize for TreeRoot {}

/// A distributed map that stores key with a fixed-size.
#[derive(Debug)]
pub struct FixedSizeIndex<'t, Key> {
    _phantom: PhantomData<Key>,
    root: TreeRoot,
    transaction: &'t Transaction,
    cache: Cache<TreeBranch>,
}

impl<'t, Key: FixedSizeKey + Send + Sync> FixedSizeIndex<'t, Key> {
    /// Instantiate a new, empty, index.
    pub async fn initialize(transaction: &'t Transaction) -> Result<Self> {
        // Let's set a max tree size of 200KB, which is a good default for AWS DynamoDB.
        let max_tree_size = 200 * 1024;
        let root = TreeRoot::new_for_max_tree_size(max_tree_size, Key::KEY_SIZE.get());

        Ok(Self {
            _phantom: Default::default(),
            root,
            transaction,
            cache: Cache::new(NonZeroUsize::new(1024).unwrap()),
        })
    }

    /// Load an index from its root id.
    pub async fn load(transaction: &'t Transaction, root_id: BlobId) -> Result<Self> {
        let root = Self::fetch_root_tx(transaction, &root_id).await?;

        Ok(Self {
            _phantom: Default::default(),
            root,
            transaction,
            cache: Cache::new(NonZeroUsize::new(1024).unwrap()),
        })
    }

    /// Save the index.
    pub async fn save(&self) -> Result<BlobId> {
        let data = self.root.to_vec();

        self.transaction
            .store(data, &StoreOptions::default())
            .await
            .map_err(Into::into)
    }

    /// Clear the index from all content.
    ///
    /// This keeps its current min and max count settings.
    pub async fn clear(&mut self) -> Result<()> {
        tracing::trace!("Clearing root.");

        if let Some(branch_id) = self.root.branch_id.take() {
            self.transaction.unstore(&branch_id).await?;
        }

        Ok(())
    }

    /// Set the balancing parameters on the tree.
    ///
    /// This dictates the average amount of elements per layer of the tree.
    ///
    /// `min_count` indicates the minimum amount of child nodes that must exist on a given layer
    /// when splitting (factorizing) the tree.
    ///
    /// `max_count` indicates the threshold over which a tree layer will be candidate for
    /// balancing.
    pub fn set_balancing_parameters(&mut self, min_count: u64, max_count: u64) -> Result<()> {
        if min_count < 1 {
            return Err(Error::InvalidParameter {
                parameter: "min_count",
                err: "must be at least 1".to_owned(),
            });
        }

        if min_count > max_count {
            return Err(Error::InvalidParameter {
                parameter: "min_count",
                err: "must be greater than `max_count`".to_owned(),
            });
        }

        self.root.min_count = min_count;
        self.root.max_count = max_count;

        Ok(())
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
        let size_diff: i128;

        let stack = match self.search(key).await? {
            TreeSearchResult::Found { stack } => {
                let (stack, (mut branch, child_key)) = stack.pop_non_empty()?;

                let mut entry = branch.occupied_entry(&child_key);
                let old_value = entry.value();

                if &value == old_value {
                    return Ok(Some(value));
                }

                // Set the parameters for the stack update.
                count_diff = 0;
                let new_size: i128 = new_size.try_into().expect("should fit a i128");
                let old_size: i128 = old_value.size().try_into().expect("should fit a i128");
                size_diff = new_size - old_size;

                result = Some(old_value.clone());

                entry.replace(value);
                branch.meta.adjust_total_size(size_diff);
                branch.meta.total_count += count_diff;
                value = self.persist_branch(branch).await?;

                // Note: since the old value is pointing to an externally provided blob id (not a
                // tree node blob id), we don't actually `unstore` it here as it is not our
                // responsibility. After all, we don't `store` the provided `value` either.

                stack
            }
            TreeSearchResult::Missing {
                mut stack,
                next_key,
            } => {
                // Set the parameters for the stack update.
                // There was no such key, so no previous value to return.
                count_diff = 1;
                size_diff = new_size.try_into().expect("should fit into a i128");
                result = None;

                // There is some more key material to create under the missing key:
                // start with this.
                let has_leafs = if let Some(next_key) = next_key {
                    let meta = TreeMeta {
                        total_size: new_size,
                        total_count: count_diff,
                    };

                    let branch = TreeBranch::new_with_single_child(meta, next_key, value);

                    value = self.persist_branch(branch).await?;

                    false
                } else {
                    true
                };

                // If we were to return the stack as-is, its top element would NOT have the
                // associated key, which is not what we want. So we pop it here and deal with
                // it as a special case so that the rest of the stack unwinding can expect the
                // child to always be there.
                if let Some((mut branch, child_key)) = stack.pop() {
                    branch.meta.total_size += new_size;
                    branch.meta.total_count += count_diff;
                    branch.insert_non_existing(child_key, value);

                    let branch = self
                        .factorize(branch, self.root.min_count, self.root.max_count, has_leafs)
                        .await?;

                    value = self.persist_branch(branch).await?;
                }

                stack
            }
        };

        // At this point, if we have stack, its top value has a child node at `child_key` that
        // needs to be either inserted or updated with the value in `value`.

        for (mut branch, child_key) in stack {
            let old_value = branch.replace_existing(&child_key, value);

            self.transaction.unstore(&old_value).await?;
            branch.meta.adjust_total_size(size_diff);
            branch.meta.total_count += count_diff;

            let branch = self
                .factorize(branch, self.root.min_count, self.root.max_count, false)
                .await?;

            value = self.persist_branch(branch).await?;
        }

        self.set_root_branch_id(Some(value)).await?;

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

                let mut value = if branch.children.is_empty() {
                    None
                } else {
                    branch.meta.total_size -= result.size();
                    branch.meta.total_count -= 1;

                    Some(self.persist_branch(branch).await?)
                };

                for (mut branch, child_key) in stack {
                    let old_value = if let Some(value) = value {
                        branch.replace_existing(&child_key, value)
                    } else {
                        branch.remove_existing(&child_key)
                    };

                    self.transaction.unstore(&old_value).await?;

                    value = if branch.children.is_empty() {
                        None
                    } else {
                        branch.meta.total_size -= result.size();
                        branch.meta.total_count -= 1;

                        let branch = self
                            .distribute(branch, self.root.min_count, self.root.max_count)
                            .await?;

                        Some(self.persist_branch(branch).await?)
                    };
                }

                self.set_root_branch_id(value).await?;

                Some(result)
            }
            TreeSearchResult::Missing { .. } => None,
        })
    }

    /// Check if the index is empty.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(match &self.root.branch_id {
            Some(branch_id) => self.fetch_branch(branch_id).await?.meta.total_count == 0,
            None => true,
        })
    }

    /// Get the count of items in the index.
    ///
    /// This size does not consider intermediate branches, just leaf nodes.
    pub async fn len(&self) -> Result<u64> {
        Ok(match &self.root.branch_id {
            Some(branch_id) => self.fetch_branch(branch_id).await?.meta.total_count,
            None => 0,
        })
    }

    /// Get the count of items in the index.
    ///
    /// This size does not consider intermediate branches, just leaf nodes.
    pub async fn total_size(&self) -> Result<u64> {
        Ok(match &self.root.branch_id {
            Some(branch_id) => self.fetch_branch(branch_id).await?.meta.total_size,
            None => 0,
        })
    }

    async fn set_root_branch_id(&mut self, branch_id: Option<BlobId>) -> Result<()> {
        if let Some(branch_id) = &self.root.branch_id {
            self.transaction.unstore(branch_id).await?;
        }

        self.root.branch_id = branch_id;

        Ok(())
    }

    async fn search(&self, key: &Key) -> Result<TreeSearchResult> {
        let key = key.to_bytes();

        let expected_key_size: usize = Key::KEY_SIZE
            .get()
            .try_into()
            .expect("key size should convert to usize");
        assert_eq!(key.len(), expected_key_size);

        match &self.root.branch_id {
            Some(branch_id) => {
                let branch = self.fetch_branch(branch_id).await?;
                let local_key_size = match branch.local_key_size() {
                    Some(local_key_size) => local_key_size,
                    None => {
                        return Err(Error::CorruptedTree {
                            path: Default::default(),
                            err: "root branch has no children".to_owned(),
                        });
                    }
                };
                let (child_key, next_key) = Self::split_key(key, local_key_size);
                let stack = TreeSearchStack::new(branch, child_key);

                self.search_from(stack, next_key).await
            }
            None => Ok(crate::indexing::TreeSearchResult::Missing {
                stack: Default::default(),
                next_key: Some(BinaryTreePathElement(key)),
            }),
        }
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
                            let local_key_size = match branch.local_key_size() {
                                Some(local_key_size) => local_key_size,
                                None => {
                                    return Err(Error::CorruptedTree {
                                        path: stack.into_path(),
                                        err: "non-root branch has no children".to_owned(),
                                    });
                                }
                            };

                            let (child_key, next_key) = Self::split_key(next_key, local_key_size);
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

    async fn fetch_root_tx(transaction: &Transaction, id: &BlobId) -> Result<TreeRoot> {
        let data = transaction.retrieve_to_memory(id).await?;

        TreeRoot::from_slice(&data).map_err(|err| Error::CorruptedTree {
            path: BinaryTreePath::root(),
            err: err.to_string(),
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
        match self.cache.get(id) {
            Some(branch) => Ok(branch.clone()),
            None => Self::fetch_branch_tx(self.transaction, id).await,
        }
    }

    async fn persist_branch(&mut self, branch: TreeBranch) -> Result<BlobId> {
        let data = branch.to_vec();

        let blob_id = self
            .transaction
            .store(data, &StoreOptions::default())
            .await?;

        self.cache.set(blob_id.clone(), branch);

        Ok(blob_id)
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

    /// Join two keys.
    fn join_key(
        left: BinaryTreePathElement,
        right: BinaryTreePathElement,
    ) -> BinaryTreePathElement {
        BinaryTreePathElement([left.0, right.0].concat().into())
    }

    /// Factorize the specified branch, using the specified parameters.
    async fn factorize(
        &mut self,
        mut branch: TreeBranch,
        min_count: u64,
        max_count: u64,
        has_leafs: bool,
    ) -> Result<TreeBranch> {
        if branch.children.len() <= max_count.try_into().expect("can convert to usize") {
            return Ok(branch);
        }
        let local_key_size = match branch.local_key_size() {
            Some(local_key_size) => local_key_size,
            None => return Ok(branch),
        };

        let mut key_size = NonZeroU64::new(1).unwrap();
        let mut buckets =
            Vec::<(BinaryTreePathElement, Vec<_>)>::with_capacity(branch.children.len());

        loop {
            buckets.clear();

            if key_size >= local_key_size {
                return Ok(branch);
            }

            for TreeItem(key, blob_id) in &branch.children {
                let (key, next_key) = Self::split_key(key.0.clone(), key_size);
                let next_key = next_key.expect("there should always be a next key");

                match buckets.last_mut() {
                    Some(last) if last.0 == key => {
                        last.1.push((next_key, blob_id));
                    }
                    _ => buckets.push((key, vec![(next_key, blob_id)])),
                };
            }

            // If we have enough different buckets, use that.
            //
            // We don't need to check if we don't have to many buckets, because we start with the
            // smaller possible key size, and as a result, any future attempt will yield at least
            // as many buckets anyway.
            if buckets.len() >= min_count.try_into().expect("can convert to usize") {
                break;
            }

            key_size = key_size
                .checked_add(1)
                .expect("addition should always be possible");
        }

        tracing::debug!(
            "Factorizing tree branch from {} children to {}.",
            branch.children.len(),
            buckets.len()
        );

        let mut children = Vec::with_capacity(buckets.len());

        for (key, sub_children) in buckets {
            let blob_id = {
                let children: Vec<TreeItem<_>> = sub_children
                    .into_iter()
                    .map(|(key, blob_id)| TreeItem(BinaryTreePathElement(key), blob_id.clone()))
                    .collect();

                let meta = if has_leafs {
                    let total_size = children.iter().map(|item| item.1.size()).sum();

                    TreeMeta {
                        total_count: children
                            .len()
                            .try_into()
                            .expect("conversion to u64 should succeed"),
                        total_size,
                    }
                } else {
                    let (mut total_count, mut total_size) = (0, 0);

                    for TreeItem(_, blob_id) in &children {
                        let branch = self.fetch_branch(blob_id).await?;
                        total_count += branch.meta.total_count;
                        total_size += branch.meta.total_size;
                    }

                    TreeMeta {
                        total_count,
                        total_size,
                    }
                };

                let branch = TreeBranch { meta, children };
                self.persist_branch(branch).await?
            };

            children.push(TreeItem(key, blob_id));
        }

        branch.children = children;

        Ok(branch)
    }

    /// Distribute the specified branch, possibly reducing its depth.
    ///
    /// This method should not be called on branches that have leaf nodes.
    async fn distribute(
        &mut self,
        mut branch: TreeBranch,
        min_count: u64,
        max_count: u64,
    ) -> Result<TreeBranch> {
        if branch.children.len() >= min_count.try_into().expect("can convert to usize")
            || branch.meta.total_count < max_count
        {
            return Ok(branch);
        }

        let mut children = Vec::with_capacity(branch.children.capacity());

        for TreeItem(key, blob_id) in &branch.children {
            let sub_branch = self.fetch_branch(blob_id).await?;

            children.reserve(sub_branch.children.len());

            for TreeItem(sub_key, blob_id) in sub_branch.children {
                let new_key = Self::join_key(key.clone(), sub_key);
                children.push(TreeItem(new_key, blob_id));
            }

            self.transaction.unstore(blob_id).await?;
        }

        branch.children = children;

        self.factorize(branch, min_count, max_count, false).await
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

        let initial_root_id = index.save().await.unwrap();

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

        let final_root_id = index.save().await.unwrap();
        assert_eq!(initial_root_id, final_root_id);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_fixed_size_index_populated() {
        let client = Client::new_for_tests();
        let tx = client.start_transaction().unwrap();

        assert_transaction_empty!(tx);

        let mut index = FixedSizeIndex::<'_, u64>::initialize(&tx).await.unwrap();

        index.set_balancing_parameters(4, 256).unwrap();

        assert_transaction_empty!(tx);

        // Insert 1024 values in the tree to trigger actual transaction writing as well as
        // rebalancing.
        for i in 0..1024 {
            assert_insert_new!(
                index,
                i,
                "this is a rather large value that is self-contained"
            );
        }

        // Since 1024 is perfectly divisible by 4 we end up with 4 root nodes that have exactly the
        // same children, hence only one actual reference (saving 75% of the space).
        //
        // So the root node + the sub-node makes 2 total references.
        assert_transaction_references_count!(tx, 2);

        for i in 0..1024 {
            assert_insert_update!(
                index,
                i,
                "this is a new value which is also rather large",
                "this is a rather large value that is self-contained"
            );
        }

        assert_transaction_references_count!(tx, 2);

        for i in 0..1024 {
            assert_remove_exists!(index, i, "this is a new value which is also rather large");
        }

        assert_transaction_empty!(tx);
        assert_empty!(index);
        assert_total_size!(index, 0);

        assert_transaction_empty!(tx);
    }

    #[test]
    fn test_tree_branch_serialization() {
        let tree_branch = TreeBranch::new(TreeMeta {
            total_count: 0,
            total_size: 0,
        });

        let buf = tree_branch.to_vec();
        assert_eq!(&[0x92, 0x92, 0x00, 0x00, 0x90], buf.as_slice());

        let mut tree_branch = TreeBranch::new(TreeMeta {
            total_count: 2000,
            total_size: 987654321,
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
                0x92, 0x92, 0xCD, 0x07, 0xD0, 0xCE, 0x3A, 0xDE, 0x68, 0xB1, 0x91, 0x92, 196, 0x11,
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                0x0E, 0x0F, 0x10, 0xA8, 0x41, 0x32, 0x5A, 0x76, 0x62, 0x77, 0x3D, 0x3D
            ],
            buf.as_slice()
        );
    }
}
