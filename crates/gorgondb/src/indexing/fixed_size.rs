use async_stream::try_stream;
use std::cmp::Ordering;
use std::future::Future;
use std::{
    borrow::Cow,
    collections::VecDeque,
    marker::PhantomData,
    mem::size_of,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};
use tokio::try_join;
use tracing::trace;

use byteorder::ByteOrder;
use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{gorgon::StoreOptions, BlobId, Transaction};

use super::{
    tree::{ByteDeserialize, ByteSerialize, TreeItem},
    BinaryTreePathElement, Cache, Error, TreeDiff,
};

/// A trait for types that can be used as a fixed-size key.
pub trait FixedSizeKey: Sized + Ord + Eq {
    /// The size of the key.
    const KEY_USIZE: usize = size_of::<Self>();
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(Self::KEY_USIZE as u64) };

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

    /// Build an instance from bytes.
    ///
    /// The passed-in value must be exactly `KEY_SIZE` bytes long or the call will panic.
    fn from_slice(buf: &[u8]) -> Self;
}

impl FixedSizeKey for u8 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(Self::KEY_USIZE as u64) };

    fn to_bytes(&self) -> Bytes {
        vec![*self].into()
    }

    fn from_slice(buf: &[u8]) -> Self {
        assert_eq!(buf.len(), size_of::<Self>());

        buf[0]
    }
}

impl FixedSizeKey for u16 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(Self::KEY_USIZE as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u16(&mut buf, *self);

        buf.into()
    }

    fn from_slice(buf: &[u8]) -> Self {
        byteorder::NetworkEndian::read_u16(buf)
    }
}

impl FixedSizeKey for u32 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(Self::KEY_USIZE as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u32(&mut buf, *self);

        buf.into()
    }

    fn from_slice(buf: &[u8]) -> Self {
        byteorder::NetworkEndian::read_u32(buf)
    }
}

impl FixedSizeKey for u64 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(Self::KEY_USIZE as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u64(&mut buf, *self);

        buf.into()
    }

    fn from_slice(buf: &[u8]) -> Self {
        byteorder::NetworkEndian::read_u64(buf)
    }
}

impl FixedSizeKey for u128 {
    /// The size of the key.
    const KEY_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(Self::KEY_USIZE as u64) };

    fn to_bytes(&self) -> Bytes {
        let mut buf = Self::make_buf();
        byteorder::NetworkEndian::write_u128(&mut buf, *self);

        buf.into()
    }

    fn from_slice(buf: &[u8]) -> Self {
        byteorder::NetworkEndian::read_u128(buf)
    }
}

type Result<T, E = Error<BinaryTreePathElement>> = std::result::Result<T, E>;
type TreePath = super::TreePath<BinaryTreePathElement>;
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
        let total_size: i128 = self.total_size.into();
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
pub struct FixedSizeIndex<Key> {
    _phantom: PhantomData<Key>,
    root: TreeRoot,
    transaction: Transaction,
    cache: Cache<Arc<TreeBranch>>,
}

impl<Key> FixedSizeIndex<Key> {
    fn default_cache() -> Cache<Arc<TreeBranch>> {
        Cache::new(NonZeroUsize::new(1024).unwrap())
    }
}

impl<Key: FixedSizeKey + Send + Sync + std::fmt::Debug> FixedSizeIndex<Key> {
    /// Instantiate a new, empty, index.
    pub fn new(transaction: Transaction) -> Self {
        // Let's set a max tree size of 200KB, which is a good default for AWS DynamoDB.
        let max_tree_size = 200 * 1024;
        let root = TreeRoot::new_for_max_tree_size(max_tree_size, Key::KEY_SIZE.get());

        Self {
            _phantom: Default::default(),
            root,
            transaction,
            cache: Self::default_cache(),
        }
    }

    /// Load an index from its root id.
    pub async fn load(transaction: Transaction, root_id: BlobId) -> Result<Self> {
        let root = Self::fetch_root_tx(&transaction, &root_id).await?;

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

    /// Fork the index, producing a new, identical index that is completely independant from the
    /// initial one.
    ///
    /// All the uncommited data both in memory or on disk will be duplicated.
    pub async fn fork(&self, transaction_name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            _phantom: PhantomData,
            root: self.root.clone(),
            transaction: self.transaction.fork(transaction_name).await,
            cache: Self::default_cache(),
        }
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

    /// Get a reference to the associated transaction.
    pub fn transaction(&self) -> &Transaction {
        &self.transaction
    }

    /// Get a value from the index.
    pub async fn get(&self, key: &Key) -> Result<Option<BlobId>> {
        Ok(match self.search(key).await? {
            TreeSearchResult::Found { stack } => {
                Some(stack.top_value().expect("entry should be occupied").clone())
            }
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
                let (stack, (branch, child_key)) = stack.pop_non_empty()?;

                let mut branch = Arc::try_unwrap(branch).unwrap_or_else(|branch| (*branch).clone());
                let mut entry = branch.occupied_entry(&child_key);
                let old_value = entry.value();

                if &value == old_value {
                    return Ok(Some(value));
                }

                // Set the parameters for the stack update.
                count_diff = 0;
                let new_size: i128 = new_size.into();
                let old_size: i128 = old_value.size().into();
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
                size_diff = new_size.into();
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
                if let Some((branch, child_key)) = stack.pop() {
                    let mut branch =
                        Arc::try_unwrap(branch).unwrap_or_else(|branch| (*branch).clone());
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

        for (branch, child_key) in stack {
            let mut branch = Arc::try_unwrap(branch).unwrap_or_else(|branch| (*branch).clone());
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
                let (stack, (branch, child_key)) = stack.pop_non_empty()?;

                let mut branch = Arc::try_unwrap(branch).unwrap_or_else(|branch| (*branch).clone());
                let result = branch.occupied_entry(&child_key).remove();

                let mut value = if branch.children.is_empty() {
                    None
                } else {
                    branch.meta.total_size -= result.size();
                    branch.meta.total_count -= 1;

                    Some(self.persist_branch(branch).await?)
                };

                for (branch, child_key) in stack {
                    let mut branch =
                        Arc::try_unwrap(branch).unwrap_or_else(|branch| (*branch).clone());
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
            match stack.top_value() {
                Some(id) => {
                    match next_key {
                        // If we have no next key, it means we found the value.
                        None => Ok(TreeSearchResult::Found { stack }),
                        Some(next_key) => {
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
            path: Default::default(),
            err: err.to_string(),
        })
    }

    async fn fetch_branch_tx(transaction: &Transaction, id: &BlobId) -> Result<TreeBranch> {
        let data = transaction.retrieve_to_memory(id).await?;

        TreeBranch::from_slice(&data).map_err(|err| Error::CorruptedTree {
            path: Default::default(),
            err: err.to_string(),
        })
    }

    async fn fetch_branch(&self, id: &BlobId) -> Result<Arc<TreeBranch>> {
        match self.cache.get(id) {
            Some(branch) => {
                trace!(
                    "cache hit while fetching branch `{id}` in index with transaction `{}`",
                    self.transaction.name()
                );

                Ok(branch.clone())
            }
            None => {
                trace!(
                    "cache miss while fetching branch `{id}` in index with transaction `{}`",
                    self.transaction.name()
                );

                Self::fetch_branch_tx(&self.transaction, id)
                    .await
                    .map(Arc::new)
            }
        }
    }

    async fn persist_branch(&mut self, branch: impl Into<Arc<TreeBranch>>) -> Result<BlobId> {
        let branch = branch.into();
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
            // We don't need to check if we don't have too many buckets, because we start with the
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

            for TreeItem(sub_key, blob_id) in &sub_branch.children {
                let new_key = Self::join_key(key.clone(), sub_key.clone());
                children.push(TreeItem(new_key, blob_id.clone()));
            }

            self.transaction.unstore(blob_id).await?;
        }

        branch.children = children;

        self.factorize(branch, min_count, max_count, false).await
    }

    /// Scan all the values in the tree.
    ///
    /// # Warning
    ///
    /// Calling this method on a big tree will take a huge amount of time and iterations. Don't use
    /// this to search for values. It's almost never what you want.
    pub fn scan(&self) -> impl Stream<Item = Result<(Key, BlobId)>> + '_ {
        try_stream! {
            match &self.root.branch_id {
                Some(branch_id) => {
                    let stream = self.scan_root(branch_id);

                    tokio::pin!(stream);

                    while let Some(item) = stream.next().await {
                        yield item?;
                    }
                }
                None => {}
            }
        }
    }

    fn scan_root<'s>(
        &'s self,
        branch_id: &'s BlobId,
    ) -> impl Stream<Item = Result<(Key, BlobId)>> + 's {
        try_stream! {
            let branch = self.fetch_branch(branch_id).await?;
            let key_prefix_path = TreePath::default();
            let key_prefix = BytesMut::new();
            let stream = self.scan_branch(key_prefix_path, key_prefix, branch);

            tokio::pin!(stream);

            while let Some(item) = stream.next().await {
                yield item?;
            }
        }
    }

    fn scan_branch(
        &self,
        key_prefix_path: TreePath,
        key_prefix: BytesMut,
        branch: Arc<TreeBranch>,
    ) -> impl Stream<Item = Result<(Key, BlobId)>> + '_ {
        let mut stack = VecDeque::new();

        stack.push_back((key_prefix_path, key_prefix, branch));

        try_stream! {
            while let Some((mut key_prefix_path, mut key_prefix, branch)) = stack.pop_front() {
                let local_key_size: usize = branch.local_key_size().ok_or_else(|| Error::CorruptedTree{
                    path: key_prefix_path.clone(),
                    err: "tree branch has no children".to_owned(),
                })?.get().try_into().expect("local key size should fit a usize");

                let key_prefix_size = key_prefix.len();
                let key_size = key_prefix_size + local_key_size;

                // If we have reached leaves...
                if key_size == Key::KEY_USIZE {
                    for TreeItem(key_elem, blob_id) in &branch.children {
                        key_prefix.extend_from_slice(&key_elem.0);
                        yield (Key::from_slice(&key_prefix), blob_id.clone());
                        key_prefix.truncate(key_prefix_size);
                    }
                } else {
                    for TreeItem(key_elem, blob_id) in &branch.children {
                        let branch = self.fetch_branch(blob_id).await?;
                        key_prefix.extend_from_slice(&key_elem.0);
                        key_prefix_path.push(key_elem.clone());

                        stack.push_back((key_prefix_path.clone(), key_prefix.clone(), branch));

                        key_prefix.truncate(key_prefix_size);
                        key_prefix_path.pop();
                    }
                }
            }
        }
    }

    /// Visit the differences between two trees.
    ///
    /// The differences are visited in strict key order.
    pub async fn visit_differences<'s>(
        &'s self,
        other: &'s Self,
        options: DiffOptions,
        visitor: impl Visitor<'s, Key>,
    ) -> Result<()> {
        let mut stack = VecDeque::new();

        match (&self.root.branch_id, &other.root.branch_id) {
            (None, None) => {
                tracing::debug!("comparing two empty indexes: no differences.");
            }
            (Some(branch_id), Some(other_branch_id)) => {
                trace!("Diffing roots `{branch_id}` and `{other_branch_id}`...");

                let left = self.fetch_branch(branch_id).await?;
                let right = other.fetch_branch(other_branch_id).await?;

                let stack_item = DiffStackItem::SamePrefix(SameSizeDiffStackItem {
                    key_prefix_path: TreePath::default(),
                    key_prefix: BytesMut::new(),
                    left,
                    right,
                });

                stack.push_back(stack_item);
            }
            (Some(branch_id), None) => {
                tracing::debug!(
                    "comparing an non-empty index `{branch_id}` with an empty index: yielding all values..."
                );

                if options.wants_left() {
                    let stream = self.scan_root(branch_id);

                    tokio::pin!(stream);

                    while let Some(item) = stream.next().await {
                        let (key, value) = item?;
                        tracing::trace!("left-only: `{key:?}` => {value}");

                        if visitor
                            .call(TreeDiff::LeftOnly { key, value })
                            .await
                            .is_stop()
                        {
                            return Ok(());
                        }
                    }
                }
            }
            (None, Some(other_branch_id)) => {
                tracing::debug!("comparing an empty index to a non-empty index `{other_branch_id}`: yielding all other values...");

                if options.wants_right() {
                    let stream = self.scan_root(other_branch_id);

                    tokio::pin!(stream);

                    while let Some(item) = stream.next().await {
                        let (key, value) = item?;
                        tracing::trace!("right-only: `{key:?}` => {value}");

                        if visitor
                            .call(TreeDiff::RightOnly { key, value })
                            .await
                            .is_stop()
                        {
                            return Ok(());
                        }
                    }
                }
            }
        };

        while let Some(diff) = stack.pop_front() {
            match diff {
                DiffStackItem::OnlyLeft(SingleDiffStackItem {
                    key_prefix_path,
                    key_prefix,
                    branch,
                }) => {
                    if options.wants_left() {
                        let stream = self.scan_branch(key_prefix_path, key_prefix, branch);

                        tokio::pin!(stream);

                        while let Some((key, value)) = stream.next().await.transpose()? {
                            tracing::trace!("left-only: `{key:?}` => {value}");

                            if visitor
                                .call(TreeDiff::LeftOnly { key, value })
                                .await
                                .is_stop()
                            {
                                return Ok(());
                            }
                        }
                    }
                }
                DiffStackItem::OnlyRight(SingleDiffStackItem {
                    key_prefix_path,
                    key_prefix,
                    branch,
                }) => {
                    if options.wants_right() {
                        let stream = other.scan_branch(key_prefix_path, key_prefix, branch);

                        tokio::pin!(stream);

                        while let Some((key, value)) = stream.next().await.transpose()? {
                            tracing::trace!("right-only: `{key:?}` => {value}");

                            if visitor
                                .call(TreeDiff::RightOnly { key, value })
                                .await
                                .is_stop()
                            {
                                return Ok(());
                            }
                        }
                    }
                }
                DiffStackItem::SamePrefix(SameSizeDiffStackItem {
                    key_prefix_path,
                    key_prefix,
                    left,
                    right,
                }) => {
                    tracing::trace!("comparing two keys with identical prefix: {key_prefix_path}");

                    // The keys have the same exact same prefix, so we can compare the children
                    // directly.

                    let mut left_children = left.children.as_slice();
                    let mut right_children = right.children.as_slice();

                    while let (
                        Some(TreeItem(left_child_key, left_child_blob_id)),
                        Some(TreeItem(right_child_key, right_child_blob_id)),
                    ) = (left_children.first(), right_children.first())
                    {
                        // While we have children on both sides, we can compare them directly.
                        tracing::trace!(
                            "comparing children `{key_prefix_path}.{left_child_key}` vs `{key_prefix_path}.{right_child_key}`..."
                        );

                        match left_child_key.cmp(right_child_key) {
                            Ordering::Equal => {
                                // Progress the children on both sides.
                                left_children = &left_children[1..];
                                right_children = &right_children[1..];

                                if left_child_blob_id == right_child_blob_id {
                                    tracing::trace!(
                                        "children `{key_prefix_path}.{left_child_key}` and `{key_prefix_path}.{right_child_key}` are the same."
                                    );

                                    // If the children are the same, we can skip them.
                                    continue;
                                }

                                tracing::trace!(
                                    "children `{key_prefix_path}.{left_child_key}` and `{key_prefix_path}.{right_child_key}` are different."
                                );

                                let mut sub_key_prefix = key_prefix.clone();
                                sub_key_prefix.extend_from_slice(&left_child_key.0);

                                if sub_key_prefix.len() == Key::KEY_USIZE {
                                    let key = Key::from_slice(&sub_key_prefix);

                                    match options {
                                        DiffOptions::Symmetric => {
                                            tracing::trace!(
                                                "symmetric difference: `{key:?}` => {left_child_blob_id} != {right_child_blob_id}"
                                            );

                                            if visitor
                                                .call(TreeDiff::Diff {
                                                    key,
                                                    left: left_child_blob_id.clone(),
                                                    right: right_child_blob_id.clone(),
                                                })
                                                .await
                                                .is_stop()
                                            {
                                                return Ok(());
                                            }
                                        }
                                        DiffOptions::LeftOnly => {
                                            tracing::trace!(
                                                "left-only: `{key:?}` => {left_child_blob_id}"
                                            );

                                            if visitor
                                                .call(TreeDiff::LeftOnly {
                                                    key,
                                                    value: left_child_blob_id.clone(),
                                                })
                                                .await
                                                .is_stop()
                                            {
                                                return Ok(());
                                            }
                                        }
                                        DiffOptions::RightOnly => {
                                            tracing::trace!(
                                                "right-only: `{key:?}` => {right_child_blob_id}"
                                            );

                                            if visitor
                                                .call(TreeDiff::RightOnly {
                                                    key,
                                                    value: right_child_blob_id.clone(),
                                                })
                                                .await
                                                .is_stop()
                                            {
                                                return Ok(());
                                            }
                                        }
                                    };
                                } else {
                                    let mut sub_key_prefix_path = key_prefix_path.clone();
                                    sub_key_prefix_path.push(left_child_key.clone());

                                    let (left, right) = try_join!(
                                        self.fetch_branch(left_child_blob_id),
                                        other.fetch_branch(right_child_blob_id)
                                    )?;

                                    stack.push_back(DiffStackItem::SamePrefix(
                                        SameSizeDiffStackItem {
                                            key_prefix_path: sub_key_prefix_path,
                                            key_prefix: sub_key_prefix,
                                            left,
                                            right,
                                        },
                                    ));
                                }
                            }
                            Ordering::Less if right_child_key.has_prefix(left_child_key) => {
                                // The left child is a prefix of the right child.
                                //
                                // We must add a deferred comparison of each of left's child
                                // children with the right child.
                                tracing::trace!(
                                    "left child `{key_prefix_path}.{left_child_key}` is a prefix of right child `{key_prefix_path}.{right_child_key}`."
                                );

                                right_children = &right_children[1..];
                                left_children = &left_children[1..];

                                let mut start_key_prefix = None;
                                let mut stop_key_prefix;
                                let mut next_pair = Some((right_child_key, right_child_blob_id));

                                while let Some((right_child_key, right_child_blob_id)) =
                                    next_pair.take()
                                {
                                    stop_key_prefix = match right_children.first() {
                                        Some(TreeItem(
                                            next_right_child_key,
                                            next_right_child_blob_id,
                                        )) if next_right_child_key.has_prefix(left_child_key) => {
                                            let mut stop_key_prefix = key_prefix.clone();
                                            stop_key_prefix
                                                .extend_from_slice(&next_right_child_key.0);

                                            next_pair = Some((
                                                next_right_child_key,
                                                next_right_child_blob_id,
                                            ));
                                            right_children = &right_children[1..];

                                            Some(stop_key_prefix)
                                        }
                                        _ => None,
                                    };

                                    // TODO: We could probably optimize this by fetching all the
                                    // children in parallel.
                                    let (left, right) = try_join!(
                                        self.fetch_branch(left_child_blob_id),
                                        other.fetch_branch(right_child_blob_id)
                                    )?;

                                    let mut right_key_prefix = key_prefix.clone();
                                    right_key_prefix.extend_from_slice(&right_child_key.0);
                                    let mut right_key_prefix_path = key_prefix_path.clone();
                                    right_key_prefix_path.push(right_child_key.clone());

                                    let mut left_key_prefix = key_prefix.clone();
                                    left_key_prefix.extend_from_slice(&left_child_key.0);
                                    let mut left_key_prefix_path = key_prefix_path.clone();
                                    left_key_prefix_path.push(left_child_key.clone());

                                    stack.push_back(DiffStackItem::ShorterLeft(
                                        AsymmetricDiffStackItem {
                                            left_key_prefix_path,
                                            left_key_prefix,
                                            left,
                                            right_key_prefix_path,
                                            right_key_prefix,
                                            right,
                                            start_key_prefix,
                                            stop_key_prefix: stop_key_prefix.clone(),
                                        },
                                    ));

                                    start_key_prefix = stop_key_prefix;
                                }
                            }
                            Ordering::Less => {
                                // The left child is smaller than the right child but not a prefix:
                                // we can return all its sub-tree as left-only.
                                tracing::trace!(
                                    "left child `{key_prefix_path}.{left_child_key}` is smaller than right child `{key_prefix_path}.{right_child_key}`."
                                );

                                left_children = &left_children[1..];

                                if options.wants_left() {
                                    let mut left_key_prefix = key_prefix.clone();
                                    left_key_prefix.extend_from_slice(&left_child_key.0);

                                    if left_key_prefix.len() == Key::KEY_USIZE {
                                        let key = Key::from_slice(&left_key_prefix);
                                        tracing::trace!(
                                            "left-only: `{key:?}` => {left_child_blob_id}"
                                        );

                                        if visitor
                                            .call(TreeDiff::LeftOnly {
                                                key,
                                                value: left_child_blob_id.clone(),
                                            })
                                            .await
                                            .is_stop()
                                        {
                                            return Ok(());
                                        }
                                    } else {
                                        let mut left_key_prefix_path = key_prefix_path.clone();
                                        left_key_prefix_path.push(left_child_key.clone());

                                        let left = self.fetch_branch(left_child_blob_id).await?;
                                        let stream = self.scan_branch(
                                            left_key_prefix_path,
                                            left_key_prefix,
                                            left,
                                        );

                                        tokio::pin!(stream);

                                        while let Some((key, value)) =
                                            stream.next().await.transpose()?
                                        {
                                            tracing::trace!("left-only: `{key:?}` => {value}");
                                            if visitor
                                                .call(TreeDiff::LeftOnly { key, value })
                                                .await
                                                .is_stop()
                                            {
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                            }
                            Ordering::Greater if left_child_key.has_prefix(right_child_key) => {
                                // The right child is a prefix of the left child.
                                //
                                // We must add a deferred comparison of each of right's child
                                // children with the left child.
                                tracing::trace!(
                                    "right child `{key_prefix_path}.{right_child_key}` is a prefix of left child `{key_prefix_path}.{left_child_key}`."
                                );

                                left_children = &left_children[1..];
                                right_children = &right_children[1..];
                                let mut start_key_prefix = None;
                                let mut stop_key_prefix;
                                let mut next_pair = Some((left_child_key, left_child_blob_id));

                                while let Some((left_child_key, left_child_blob_id)) =
                                    next_pair.take()
                                {
                                    stop_key_prefix = match left_children.first() {
                                        Some(TreeItem(
                                            next_left_child_key,
                                            next_left_child_blob_id,
                                        )) if next_left_child_key.has_prefix(right_child_key) => {
                                            let mut stop_key_prefix = key_prefix.clone();
                                            stop_key_prefix
                                                .extend_from_slice(&next_left_child_key.0);

                                            next_pair = Some((
                                                next_left_child_key,
                                                next_left_child_blob_id,
                                            ));
                                            left_children = &left_children[1..];

                                            Some(stop_key_prefix)
                                        }
                                        _ => None,
                                    };

                                    // TODO: We could probably optimize this by fetching all the
                                    // children in parallel.
                                    let (left, right) = try_join!(
                                        self.fetch_branch(left_child_blob_id),
                                        other.fetch_branch(right_child_blob_id)
                                    )?;

                                    let mut left_key_prefix = key_prefix.clone();
                                    left_key_prefix.extend_from_slice(&left_child_key.0);
                                    let mut left_key_prefix_path = key_prefix_path.clone();
                                    left_key_prefix_path.push(left_child_key.clone());

                                    let mut right_key_prefix = key_prefix.clone();
                                    right_key_prefix.extend_from_slice(&right_child_key.0);
                                    let mut right_key_prefix_path = key_prefix_path.clone();
                                    right_key_prefix_path.push(right_child_key.clone());

                                    stack.push_back(DiffStackItem::ShorterRight(
                                        AsymmetricDiffStackItem {
                                            left_key_prefix_path,
                                            left_key_prefix,
                                            left,
                                            right_key_prefix_path,
                                            right_key_prefix,
                                            right,
                                            start_key_prefix,
                                            stop_key_prefix: stop_key_prefix.clone(),
                                        },
                                    ));

                                    start_key_prefix = stop_key_prefix;
                                }
                            }
                            Ordering::Greater => {
                                // The right child is smaller than the left child but not a prefix:
                                // we can return all its sub-tree as right-only.
                                tracing::trace!(
                                    "right child `{key_prefix_path}.{right_child_key}` is smaller than left child `{key_prefix_path}.{left_child_key}`."
                                );

                                right_children = &right_children[1..];

                                if options.wants_right() {
                                    let mut right_key_prefix = key_prefix.clone();
                                    right_key_prefix.extend_from_slice(&right_child_key.0);

                                    if right_key_prefix.len() == Key::KEY_USIZE {
                                        let key = Key::from_slice(&right_key_prefix);
                                        tracing::trace!(
                                            "right-only: `{key:?}` => {right_child_blob_id}"
                                        );

                                        if visitor
                                            .call(TreeDiff::RightOnly {
                                                key,
                                                value: right_child_blob_id.clone(),
                                            })
                                            .await
                                            .is_stop()
                                        {
                                            return Ok(());
                                        }
                                    } else {
                                        let mut right_key_prefix_path = key_prefix_path.clone();
                                        right_key_prefix_path.push(right_child_key.clone());

                                        let right = other.fetch_branch(right_child_blob_id).await?;
                                        let stream = other.scan_branch(
                                            right_key_prefix_path,
                                            right_key_prefix,
                                            right,
                                        );

                                        tokio::pin!(stream);

                                        while let Some((key, value)) =
                                            stream.next().await.transpose()?
                                        {
                                            tracing::trace!("right-only: `{key:?}` => {value}");
                                            if visitor
                                                .call(TreeDiff::RightOnly { key, value })
                                                .await
                                                .is_stop()
                                            {
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    tracing::trace!(
                        "same prefix children comparison complete: yielding potential remaining children..."
                    );

                    if options.wants_left() {
                        // The leftover children in the left tree are necessarily greater than the rightmost
                        // children in the right tree branch: return all these children.
                        for TreeItem(left_child_key, left_child_blob_id) in left_children {
                            let mut left_key_prefix = key_prefix.clone();
                            left_key_prefix.extend_from_slice(&left_child_key.0);

                            if left_key_prefix.len() == Key::KEY_USIZE {
                                let key = Key::from_slice(&left_key_prefix);
                                tracing::trace!("left-only: `{key:?}` => {left_child_blob_id}");

                                if visitor
                                    .call(TreeDiff::LeftOnly {
                                        key,
                                        value: left_child_blob_id.clone(),
                                    })
                                    .await
                                    .is_stop()
                                {
                                    return Ok(());
                                }
                            } else {
                                let mut left_key_prefix_path = key_prefix_path.clone();
                                left_key_prefix_path.push(left_child_key.clone());

                                let left = self.fetch_branch(left_child_blob_id).await?;
                                let stream =
                                    self.scan_branch(left_key_prefix_path, left_key_prefix, left);

                                tokio::pin!(stream);

                                while let Some((key, value)) = stream.next().await.transpose()? {
                                    tracing::trace!("left-only: `{key:?}` => {value}");

                                    if visitor
                                        .call(TreeDiff::LeftOnly { key, value })
                                        .await
                                        .is_stop()
                                    {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }

                    if options.wants_right() {
                        for TreeItem(right_child_key, right_child_blob_id) in right_children {
                            let mut right_key_prefix = key_prefix.clone();
                            right_key_prefix.extend_from_slice(&right_child_key.0);

                            if right_key_prefix.len() == Key::KEY_USIZE {
                                let key = Key::from_slice(&right_key_prefix);
                                tracing::trace!("left-only: `{key:?}` => {right_child_blob_id}");

                                if visitor
                                    .call(TreeDiff::RightOnly {
                                        key,
                                        value: right_child_blob_id.clone(),
                                    })
                                    .await
                                    .is_stop()
                                {
                                    return Ok(());
                                }
                            } else {
                                let mut right_key_prefix_path = key_prefix_path.clone();
                                right_key_prefix_path.push(right_child_key.clone());

                                let right = other.fetch_branch(right_child_blob_id).await?;
                                let stream = other.scan_branch(
                                    right_key_prefix_path,
                                    right_key_prefix,
                                    right,
                                );

                                tokio::pin!(stream);

                                while let Some((key, value)) = stream.next().await.transpose()? {
                                    tracing::trace!("right-only: `{key:?}` => {value}");

                                    if visitor
                                        .call(TreeDiff::RightOnly { key, value })
                                        .await
                                        .is_stop()
                                    {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
                DiffStackItem::ShorterLeft(AsymmetricDiffStackItem {
                    left_key_prefix_path,
                    left_key_prefix,
                    left,
                    right_key_prefix_path,
                    right_key_prefix,
                    right,
                    start_key_prefix,
                    stop_key_prefix,
                }) => {
                    let left_children = left.children.as_slice();

                    for TreeItem(sub_key, sub_blob_id) in left_children {
                        let mut sub_key_prefix = left_key_prefix.clone();
                        sub_key_prefix.extend_from_slice(&sub_key.0);

                        match sub_key_prefix.cmp(&right_key_prefix) {
                            Ordering::Equal => {
                                tracing::trace!("child has reached same prefix...");

                                let left = self.fetch_branch(sub_blob_id).await?;

                                stack.push_back(DiffStackItem::SamePrefix(SameSizeDiffStackItem {
                                    key_prefix_path: right_key_prefix_path.clone(),
                                    key_prefix: right_key_prefix.clone(),
                                    left,
                                    right: right.clone(),
                                }));
                            }
                            Ordering::Less if right_key_prefix.starts_with(&sub_key_prefix) => {
                                tracing::trace!("child is still a sub-prefix...");

                                // The left sub-tree is still a prefix of the right sub-tree.
                                let left = self.fetch_branch(sub_blob_id).await?;

                                let mut sub_key_prefix_path = left_key_prefix_path.clone();
                                sub_key_prefix_path.push(sub_key.clone());

                                stack.push_back(DiffStackItem::ShorterLeft(
                                    AsymmetricDiffStackItem {
                                        left_key_prefix_path: sub_key_prefix_path,
                                        left_key_prefix: sub_key_prefix,
                                        left,
                                        right_key_prefix_path: right_key_prefix_path.clone(),
                                        right_key_prefix: right_key_prefix.clone(),
                                        right: right.clone(),
                                        start_key_prefix: start_key_prefix.clone(),
                                        stop_key_prefix: stop_key_prefix.clone(),
                                    },
                                ));
                            }
                            Ordering::Less => {
                                match &start_key_prefix {
                                    Some(start_key_prefix) if sub_key_prefix < start_key_prefix => {
                                        tracing::trace!("left child has not yet reached start key prefix: skipping it.");

                                        break;
                                    }
                                    _ => {
                                        tracing::trace!("child is less and not a sub-prefix...");
                                    }
                                };

                                if options.wants_left() {
                                    let mut sub_key_prefix_path = left_key_prefix_path.clone();
                                    sub_key_prefix_path.push(sub_key.clone());

                                    let left = self.fetch_branch(sub_blob_id).await?;
                                    let children =
                                        self.scan_branch(sub_key_prefix_path, sub_key_prefix, left);

                                    tokio::pin!(children);

                                    while let Some((key, value)) =
                                        children.next().await.transpose()?
                                    {
                                        tracing::trace!("left-only: `{key:?}` => {value}");

                                        if visitor
                                            .call(TreeDiff::LeftOnly { key, value })
                                            .await
                                            .is_stop()
                                        {
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Ordering::Greater => {
                                match &stop_key_prefix {
                                    Some(stop_key_prefix) if sub_key_prefix >= stop_key_prefix => {
                                        tracing::trace!("left child has reached stop key prefix: stopping iteration.");

                                        break;
                                    }
                                    _ => {
                                        tracing::trace!(
                                            "child is greater and hasn't reached the stop key..."
                                        );
                                    }
                                };

                                if options.wants_right() {
                                    stack.push_back(DiffStackItem::OnlyRight(
                                        SingleDiffStackItem {
                                            key_prefix_path: right_key_prefix_path.clone(),
                                            key_prefix: right_key_prefix.clone(),
                                            branch: right.clone(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
                DiffStackItem::ShorterRight(AsymmetricDiffStackItem {
                    left_key_prefix_path,
                    left_key_prefix,
                    left,
                    right_key_prefix_path,
                    right_key_prefix,
                    right,
                    start_key_prefix,
                    stop_key_prefix,
                }) => {
                    let right_children = right.children.as_slice();

                    tracing::trace!(
                        "comparing children of shorter right branch `{right_key_prefix_path}` with left branch `{left_key_prefix_path}`..."
                    );

                    for TreeItem(sub_key, sub_blob_id) in right_children {
                        let mut sub_key_prefix = right_key_prefix.clone();
                        sub_key_prefix.extend_from_slice(&sub_key.0);

                        match sub_key_prefix.cmp(&left_key_prefix) {
                            Ordering::Equal => {
                                tracing::trace!("child has reached same prefix...");

                                let right = other.fetch_branch(sub_blob_id).await?;

                                stack.push_back(DiffStackItem::SamePrefix(SameSizeDiffStackItem {
                                    key_prefix_path: left_key_prefix_path.clone(),
                                    key_prefix: left_key_prefix.clone(),
                                    left: left.clone(),
                                    right,
                                }));
                            }
                            Ordering::Less if left_key_prefix.starts_with(&sub_key_prefix) => {
                                tracing::trace!("child is still a sub-prefix...");

                                // The right sub-tree is still a prefix of the left sub-tree.
                                let right = other.fetch_branch(sub_blob_id).await?;

                                let mut sub_key_prefix_path = right_key_prefix_path.clone();
                                sub_key_prefix_path.push(sub_key.clone());

                                stack.push_back(DiffStackItem::ShorterRight(
                                    AsymmetricDiffStackItem {
                                        left_key_prefix_path: left_key_prefix_path.clone(),
                                        left_key_prefix: left_key_prefix.clone(),
                                        left: left.clone(),
                                        right_key_prefix_path: sub_key_prefix_path,
                                        right_key_prefix: sub_key_prefix,
                                        right,
                                        start_key_prefix: start_key_prefix.clone(),
                                        stop_key_prefix: stop_key_prefix.clone(),
                                    },
                                ));
                            }
                            Ordering::Less => {
                                match &start_key_prefix {
                                    Some(start_key_prefix) if sub_key_prefix < start_key_prefix => {
                                        tracing::trace!("right child has not yet reached start key prefix: skipping it.");

                                        break;
                                    }
                                    _ => {
                                        tracing::trace!("child is less and not a sub-prefix...");
                                    }
                                };

                                if options.wants_right() {
                                    let mut sub_key_prefix_path = right_key_prefix_path.clone();
                                    sub_key_prefix_path.push(sub_key.clone());

                                    //FIXME: this will likely create a tree corruption: we need to
                                    //check if we reached a leaf before fetching the branch.
                                    let right = other.fetch_branch(sub_blob_id).await?;
                                    let children = other.scan_branch(
                                        sub_key_prefix_path,
                                        sub_key_prefix,
                                        right,
                                    );

                                    tokio::pin!(children);

                                    while let Some((key, value)) =
                                        children.next().await.transpose()?
                                    {
                                        tracing::trace!("right-only: `{key:?}` => {value}");

                                        if visitor
                                            .call(TreeDiff::RightOnly { key, value })
                                            .await
                                            .is_stop()
                                        {
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Ordering::Greater => {
                                match &stop_key_prefix {
                                    Some(stop_key_prefix) if sub_key_prefix >= stop_key_prefix => {
                                        tracing::trace!("right child has reached stop key prefix: stopping iteration.");

                                        break;
                                    }
                                    _ => {
                                        tracing::trace!(
                                            "child is greater and hasn't reached the stop key..."
                                        );
                                    }
                                };

                                if options.wants_left() {
                                    stack.push_back(DiffStackItem::OnlyLeft(SingleDiffStackItem {
                                        key_prefix_path: left_key_prefix_path.clone(),
                                        key_prefix: left_key_prefix.clone(),
                                        branch: left.clone(),
                                    }));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// A visitor for tree differences.
pub trait Visitor<'s, Key> {
    /// The future type returned by the visitor.
    type Future: Future<Output = VisitResult> + 's;

    /// Call the visitor with the specified tree difference.
    fn call(&self, diff: TreeDiff<Key>) -> Self::Future;
}

impl<'s, Key, Fut: Future<Output = VisitResult> + 's, F: Fn(TreeDiff<Key>) -> Fut> Visitor<'s, Key>
    for F
{
    type Future = Fut;

    fn call(&self, diff: TreeDiff<Key>) -> Self::Future {
        self(diff)
    }
}

/// The options for diffing two trees.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DiffOptions {
    /// Symmetric diffing.
    #[default]
    Symmetric,

    /// Left-only diffing.
    LeftOnly,

    /// Right-only diffing.
    RightOnly,
}

impl DiffOptions {
    /// Check if the diffing options want the left tree.
    const fn wants_left(&self) -> bool {
        match self {
            DiffOptions::Symmetric | DiffOptions::LeftOnly => true,
            DiffOptions::RightOnly => false,
        }
    }

    /// Check if the diffing options want the right tree.
    const fn wants_right(&self) -> bool {
        match self {
            DiffOptions::Symmetric | DiffOptions::RightOnly => true,
            DiffOptions::LeftOnly => false,
        }
    }
}

/// A visit result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisitResult {
    /// Continue visiting the tree.
    Continue,

    /// Stop visiting the tree.
    Stop,
}

impl VisitResult {
    /// Check if the visit result is to continue.
    pub fn is_continue(&self) -> bool {
        matches!(self, VisitResult::Continue)
    }

    /// Check if the visit result is to stop.
    pub fn is_stop(&self) -> bool {
        matches!(self, VisitResult::Stop)
    }
}

enum DiffStackItem {
    SamePrefix(SameSizeDiffStackItem),
    OnlyLeft(SingleDiffStackItem),
    OnlyRight(SingleDiffStackItem),
    ShorterLeft(AsymmetricDiffStackItem),
    ShorterRight(AsymmetricDiffStackItem),
}

struct SingleDiffStackItem {
    key_prefix_path: TreePath,
    key_prefix: BytesMut,
    branch: Arc<TreeBranch>,
}

struct SameSizeDiffStackItem {
    key_prefix_path: TreePath,
    key_prefix: BytesMut,
    left: Arc<TreeBranch>,
    right: Arc<TreeBranch>,
}

struct AsymmetricDiffStackItem {
    left_key_prefix_path: TreePath,
    left_key_prefix: BytesMut,
    left: Arc<TreeBranch>,
    right_key_prefix_path: TreePath,
    right_key_prefix: BytesMut,
    right: Arc<TreeBranch>,
    start_key_prefix: Option<BytesMut>,
    stop_key_prefix: Option<BytesMut>,
}

#[cfg(test)]
mod tests {
    use crate::Client;
    use futures::TryStreamExt;
    use tracing_test::traced_test;

    use super::super::tests::*;
    use super::*;

    #[traced_test]
    #[tokio::test]
    async fn test_fixed_size_index() {
        let client = Client::new_for_tests();
        let tx = client.start_transaction("tx1").unwrap();

        let mut index = FixedSizeIndex::<u16>::new(tx);

        assert_transaction_empty!(index);

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

        assert_transaction_empty!(index);

        let final_root_id = index.save().await.unwrap();
        assert_eq!(initial_root_id, final_root_id);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_fixed_size_index_populated() {
        let client = Client::new_for_tests();
        let tx = client.start_transaction("tx1").unwrap();

        let mut index = FixedSizeIndex::<u64>::new(tx);

        index.set_balancing_parameters(4, 256).unwrap();

        assert_transaction_empty!(index);

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
        assert_transaction_references_count!(index, 2);

        for i in 0..1024 {
            assert_insert_update!(
                index,
                i,
                "this is a new value which is also rather large",
                "this is a rather large value that is self-contained"
            );
        }

        assert_transaction_references_count!(index, 2);

        for i in 0..1024 {
            assert_remove_exists!(index, i, "this is a new value which is also rather large");
        }

        assert_transaction_empty!(index);
        assert_empty!(index);
        assert_total_size!(index, 0);

        assert_transaction_empty!(index);
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

    #[traced_test]
    #[tokio::test]
    async fn test_fixed_size_index_scan() {
        let client = Client::new_for_tests();
        let tx = client.start_transaction("tx1").unwrap();

        let mut index = FixedSizeIndex::<u64>::new(tx);

        index.set_balancing_parameters(4, 256).unwrap();

        assert_transaction_empty!(index);

        let all: Vec<_> = index.scan().try_collect().await.unwrap();
        assert_eq!(all, vec![]);

        // Insert 1024 values in the tree to trigger actual transaction writing as well as
        // rebalancing.
        for i in 0..1024 {
            assert_insert_new!(
                index,
                i,
                "this is a rather large value that is self-contained"
            );
        }

        let all: Vec<_> = index.scan().try_collect().await.unwrap();
        assert_eq!(
            all,
            (0..1024)
                .map(|i| (
                    i,
                    blob_id!("this is a rather large value that is self-contained")
                ))
                .collect::<Vec<_>>()
        );
    }

    macro_rules! assert_diff {
        ($index:expr, $diff:expr) => {
            let result = collect_diff(&$index, &$index).await;
            assert_eq!(&result, $diff);
        };
        ($left:expr, $right:expr, $diff:expr) => {
            let result = collect_diff(&$left, &$right).await;
            assert_eq!(&result, &$diff);

            let result = collect_diff(&$right, &$left).await;
            assert_eq!(&result, &mirror_diff($diff));
        };
    }

    fn mirror_diff(diff: Vec<TreeDiff<u64>>) -> Vec<TreeDiff<u64>> {
        diff.into_iter()
            .map(|diff| match diff {
                TreeDiff::LeftOnly { key, value } => TreeDiff::RightOnly { key, value },
                TreeDiff::RightOnly { key, value } => TreeDiff::LeftOnly { key, value },
                TreeDiff::Diff { key, left, right } => TreeDiff::Diff {
                    key,
                    left: right,
                    right: left,
                },
            })
            .collect()
    }

    async fn collect_diff<Key: FixedSizeKey + Send + Sync + std::fmt::Debug>(
        left: &FixedSizeIndex<Key>,
        right: &FixedSizeIndex<Key>,
    ) -> Vec<TreeDiff<Key>> {
        let results = tokio::sync::Mutex::new(Vec::with_capacity(4));

        left.visit_differences(right, DiffOptions::Symmetric, |diff| {
            Box::pin(async {
                let mut results = results.lock().await;

                results.push(diff);

                assert!(results.len() < 10);

                VisitResult::Continue
            })
        })
        .await
        .unwrap();

        results.into_inner()
    }

    #[traced_test]
    #[tokio::test]
    async fn test_fixed_size_index_diff() {
        let client = Client::new_for_tests();
        let tx = client.start_transaction("tx1").unwrap();

        let mut index = FixedSizeIndex::<u64>::new(tx);

        index.set_balancing_parameters(4, 256).unwrap();

        assert_transaction_empty!(index);

        // Make sure a diff with ourselves yields no result.
        assert_diff!(index, &[]);

        // Insert 1023 values in the tree to trigger actual transaction writing as well as
        // rebalancing.
        for i in 1..1024 {
            assert_insert_new!(index, i, "value");
        }

        // Make sure a diff with ourselves yields no result.
        assert_diff!(index, &[]);

        {
            let mut index2 = index.fork("tx2").await;

            assert_insert_new!(index2, 0, "new-value");
            assert_insert_update!(index2, 16, "other-value", "value");
            assert_remove_exists!(index2, 128, "value");

            assert_diff!(
                index,
                index2,
                vec![
                    TreeDiff::RightOnly {
                        key: 0,
                        value: blob_id!("new-value")
                    },
                    TreeDiff::Diff {
                        key: 16,
                        left: blob_id!("value"),
                        right: blob_id!("other-value")
                    },
                    TreeDiff::LeftOnly {
                        key: 128,
                        value: blob_id!("value")
                    },
                ]
            );
        }

        {
            // Test comparison of trees that have the same semantic content but vastly different
            // structures due to different balancing parameters.
            let mut index3 = FixedSizeIndex::<u64>::new(index.transaction().fork("tx3").await);

            index3.set_balancing_parameters(1, 4).unwrap();

            for i in 1..1024 {
                assert_insert_new!(index3, i, "value");
            }

            assert_diff!(index, index3, vec![]);

            assert_insert_new!(index3, 0, "new-value");
            assert_insert_update!(index3, 16, "other-value", "value");
            assert_remove_exists!(index3, 128, "value");

            assert_diff!(
                index,
                index3,
                vec![
                    TreeDiff::RightOnly {
                        key: 0,
                        value: blob_id!("new-value")
                    },
                    TreeDiff::Diff {
                        key: 16,
                        left: blob_id!("value"),
                        right: blob_id!("other-value")
                    },
                    TreeDiff::LeftOnly {
                        key: 128,
                        value: blob_id!("value")
                    },
                ]
            );
        }
    }
}
