use chrono::{DateTime, Duration, Utc};
use std::{
    borrow::Cow,
    cmp::{max, min},
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tracing::warn;

use crate::RemoteRef;

use super::FilesystemStorage;

/// An storage that stores a subset of its elements in memory, keeping first the most recently
/// referenced ones.
#[derive(Debug)]
pub struct Cache {
    max_size: usize,
    impl_: Arc<Mutex<CacheImpl>>,
    filesystem_storage: Option<FilesystemStorage>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            max_size: Self::DEFAULT_MAX_SIZE,
            impl_: Default::default(),
            filesystem_storage: None,
        }
    }
}

impl Cache {
    const DEFAULT_MAX_SIZE: usize = 64 * 1024 * 1024; // 64MB

    /// Clear the cache.
    pub fn clear(&self) {
        self.impl_.lock().expect("failed to lock").clear();
    }

    /// Set the cache's maximum size for in-memory values.
    ///
    /// If the maximum size is decremented, pruning of values happens immediately.
    ///
    /// `set_max_size` does not affect the disk cache.
    pub fn set_max_size(&mut self, mut max_size: usize) -> &mut Self {
        std::mem::swap(&mut self.max_size, &mut max_size);

        if max_size > self.max_size {
            self.impl_
                .lock()
                .expect("failed to lock")
                .prune(self.max_size);
        }

        self
    }

    /// Set or reset the filesystem storage used by this cache instance.
    ///
    /// If a filesystem storage is set, it will be used to store all values, inconditionally.
    pub fn set_filesystem_storage(
        &mut self,
        filesystem_storage: Option<FilesystemStorage>,
    ) -> &mut Self {
        self.filesystem_storage = filesystem_storage;

        self
    }

    /// Set the expiration respite values on the cache.
    ///
    /// Max should never be smaller than increment.
    pub fn set_expiration_respite(&self, increment: Duration, max: Duration) -> &Self {
        assert!(increment <= max, "increment should be smaller than max");

        let mut impl_ = self.impl_.lock().expect("failed to lock");

        impl_.expiration_respite_increment = increment;
        impl_.max_expiration_respite = max;

        self
    }

    /// Retrieve the value from the cache, if it exists.
    pub async fn retrieve<'s>(&'s self, remote_ref: &RemoteRef) -> Option<crate::AsyncSource<'s>> {
        let ref_size: usize = remote_ref
            .ref_size()
            .try_into()
            .expect("cannot convert u64 to usize");

        if ref_size <= self.max_size {
            let mut impl_ = self.impl_.lock().expect("failed to lock");

            if let Some(data) = impl_.get_and_extend(remote_ref) {
                return Some(data.into());
            }
        }

        // If we have a filesystem storage, use it as a fallback to the memory one.
        if let Some(filesystem_storage) = &self.filesystem_storage {
            return match filesystem_storage.retrieve(remote_ref).await {
                Ok(Some(source)) => match source.read_all_into_memory().await {
                    Ok(data) => {
                        // We got the data from the filesystem: persist it to memory.
                        let data: Cow<'_, [u8]> = Cow::Owned(data);

                        let mut impl_ = self.impl_.lock().expect("failed to lock");

                        impl_.store(remote_ref.clone(), data.clone());
                        Some(data.into())
                    }
                    Err(err) => {
                        warn!("Failed to read from filesystem storage: {err}");

                        None
                    }
                },
                Ok(None) => None,
                Err(err) => {
                    warn!("Failed to retrieve value from filesystem cache: {err}");

                    None
                }
            };
        }

        None
    }

    /// Store the value in the cache and returns an `AsyncSource` to the cache entry.
    ///
    /// If the value is too big to fit in the cache the initial `AsyncSource` is returned instead.
    pub async fn store<'s>(
        &self,
        remote_ref: &RemoteRef,
        source: crate::AsyncSource<'s>,
    ) -> super::Result<crate::AsyncSource<'s>> {
        let ref_size: usize = remote_ref
            .ref_size()
            .try_into()
            .expect("cannot convert u64 to usize");

        if ref_size < self.max_size {
            let data = source.read_all_into_owned_memory().await?;

            assert_eq!(data.len(), ref_size, "data size does not match remote ref");

            {
                let mut impl_ = self.impl_.lock().expect("failed to lock");

                impl_.store(remote_ref.clone(), data.clone());
                impl_.prune(self.max_size);
            }

            if let Some(filesystem_storage) = &self.filesystem_storage {
                filesystem_storage.store(remote_ref, data.clone()).await?;
            }

            Ok(data.into())
        } else if let Some(filesystem_storage) = &self.filesystem_storage {
            match source.data() {
                Some(data) => {
                    // If the source is already in memory: don't use the filesystem source as a
                    // result as it certainly be slower.
                    filesystem_storage.store(remote_ref, data).await?;

                    Ok(source)
                }
                None => filesystem_storage
                    .store(remote_ref, source)
                    .await
                    .map(Into::into)
                    .map_err(Into::into),
            }
        } else {
            Ok(source)
        }
    }
}

/// A low-level storage for in-memory values that can expire.
///
/// Whenever values are read or stored, their expiration date gets extended by the configured
/// `expiration_respite_increment` up to a maximum of `max_expiration_respite`. In effect, values
/// that are frequently read or written are guaranteed to be kept around longer than others.
#[derive(Debug)]
struct CacheImpl {
    expiration_respite_increment: Duration,
    max_expiration_respite: Duration,
    by_id: HashMap<RemoteRef, IndexEntry>,
    by_expiration: ByExpiration,
    total_size: usize,
}

#[derive(Debug)]
struct IndexEntry {
    data: Cow<'static, [u8]>,
    expiration: DateTime<Utc>,
}

impl Default for CacheImpl {
    fn default() -> Self {
        let expiration_respite_increment = Duration::minutes(15);
        let max_expiration_respite = Duration::hours(24);

        Self {
            by_id: Default::default(),
            by_expiration: Default::default(),
            expiration_respite_increment,
            max_expiration_respite,
            total_size: 0,
        }
    }
}

impl CacheImpl {
    /// Clear all the cache entries, effectively reclaiming all its memory.
    pub fn clear(&mut self) {
        self.by_id.clear();
        self.by_expiration.clear();
        self.total_size = 0;
    }

    /// Tentatively store the specified value in the index.
    ///
    /// If the specified value is bigger than the index maximum size, the implementation will not
    /// attempt to store the value a simply ignore the call as this value would get evicted
    /// immediately anyway.
    pub fn store(&mut self, remote_ref: RemoteRef, data: Cow<'static, [u8]>) {
        use std::collections::hash_map::Entry;

        match self.by_id.entry(remote_ref) {
            Entry::Occupied(mut entry) => {
                debug_assert_eq!(entry.get().data, data);

                // The entry exists: extend its expiration.
                let now = Utc::now();
                let expiration = min(
                    max(entry.get().expiration, now) + self.expiration_respite_increment,
                    now + self.max_expiration_respite,
                );

                let old_expiration = std::mem::replace(&mut entry.get_mut().expiration, expiration);

                self.by_expiration
                    .replace_existing(old_expiration, expiration, entry.key());
            }
            Entry::Vacant(entry) => {
                // The entry does not exist yet: add it to all indexes.
                let now = Utc::now();
                let expiration = now
                    + min(
                        self.expiration_respite_increment,
                        self.max_expiration_respite,
                    );

                let remote_ref = entry.key();
                self.by_expiration
                    .add_non_existing(expiration, remote_ref.clone());

                self.total_size += data.len();

                entry.insert(IndexEntry { data, expiration });
            }
        }
    }

    /// Get the value with the specified remote reference, and extend its expiration.
    pub fn get_and_extend(&mut self, remote_ref: &RemoteRef) -> Option<Cow<'static, [u8]>> {
        self.by_id.get_mut(remote_ref).map(|entry| {
            // The entry exists: extend its expiration.
            let now = Utc::now();
            let expiration = min(
                max(entry.expiration, now) + self.expiration_respite_increment,
                now + self.max_expiration_respite,
            );

            let old_expiration = std::mem::replace(&mut entry.expiration, expiration);

            self.by_expiration
                .replace_existing(old_expiration, expiration, remote_ref);

            entry.data.clone()
        })
    }

    /// Prune the values in the index, starting with the ones that expire the soonest, up until the
    /// total size goes below the configured maximum size.
    pub fn prune(&mut self, max_size: usize) {
        if self.total_size > max_size {
            let size_to_reclaim = self.total_size - max_size;

            for remote_ref in &self.by_expiration.reclaim(size_to_reclaim) {
                self.by_id
                    .remove(remote_ref)
                    .expect("reference should exist");
            }
        }
    }
}

#[derive(Debug, Default)]
struct ByExpiration(BTreeMap<DateTime<Utc>, (u64, VecDeque<RemoteRef>)>);

impl ByExpiration {
    /// Clear all entries.
    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }

    /// Add a new remote reference to the instance.
    ///
    /// The remote reference must not already exist.
    ///
    /// If other elements with the same expiration exist, this new entry is considered more recent.
    pub(crate) fn add_non_existing(&mut self, expiration: DateTime<Utc>, remote_ref: RemoteRef) {
        use std::collections::btree_map::Entry;

        match self.0.entry(expiration) {
            Entry::Vacant(entry) => {
                entry.insert((remote_ref.ref_size(), [remote_ref].into()));
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();

                entry.0 += remote_ref.ref_size();
                entry.1.push_back(remote_ref);
            }
        }
    }

    /// Remove an existing remote reference from the instance.
    ///
    /// The remote reference must exist and have the specified expiration. It is returned.
    #[must_use]
    pub(crate) fn remove_existing(
        &mut self,
        expiration: DateTime<Utc>,
        remote_ref: &RemoteRef,
    ) -> RemoteRef {
        let entry = self.0.get_mut(&expiration).expect("the entry must exist");

        for (i, value) in entry.1.iter().enumerate() {
            if value == remote_ref {
                entry.0 -= remote_ref.ref_size();
                let result = entry.1.remove(i).expect("entry must exist");

                debug_assert_eq!(&result, remote_ref);

                if entry.0 == 0 {
                    debug_assert!(entry.1.is_empty());

                    self.0.remove(&expiration);
                }

                return result;
            }
        }

        panic!("entry did not exist in the queue");
    }

    /// Replace the expiration for an existing entry.
    ///
    /// The old entry must exist.
    pub(crate) fn replace_existing(
        &mut self,
        old_expiration: DateTime<Utc>,
        new_expiration: DateTime<Utc>,
        remote_ref: &RemoteRef,
    ) {
        let key = self.remove_existing(old_expiration, remote_ref);
        self.add_non_existing(new_expiration, key);
    }

    /// Remove elements that expire the soonest, until the specified memory size has been
    /// reclaimed.
    ///
    /// Deleted elements are returned in no particular order.
    pub(crate) fn reclaim(&mut self, size_to_reclaim: usize) -> Vec<RemoteRef> {
        let mut size_to_reclaim: u64 = size_to_reclaim
            .try_into()
            .expect("failed to convert usize to u64");

        let mut result = vec![];
        let mut expirations_to_remove = vec![];
        let mut additional = 0;

        for (expiration, (queue_size, queue)) in self.0.iter_mut() {
            // Fast-case: the entire queue size is smaller than the size to reclaim: we can just
            // delete the queue altogether.
            if *queue_size <= size_to_reclaim {
                size_to_reclaim -= *queue_size;
                expirations_to_remove.push(*expiration);
                additional += queue.len();

                continue;
            } else if queue.len() == 1 && size_to_reclaim > 0 {
                // Special case: the queue has more memory than what's left to reclaim but it
                // contains only one element: we can pop the entire queue and stop the iteration
                // there.
                expirations_to_remove.push(*expiration);
                additional += queue.len();

                break;
            }

            let mut i = 0;

            for remote_ref in queue.iter_mut() {
                if size_to_reclaim == 0 {
                    break;
                }

                i += 1;

                if remote_ref.ref_size() < size_to_reclaim {
                    size_to_reclaim -= remote_ref.ref_size();
                } else {
                    size_to_reclaim = 0;
                }
            }

            result.reserve(i);

            while i > 0 {
                let item = queue.remove(0).expect("queue should not be empty");
                *queue_size -= item.ref_size();
                result.push(item);
                i -= 1;
            }

            queue.make_contiguous();

            break;
        }

        result.reserve(additional);

        for expiration in &expirations_to_remove {
            result.extend(
                self.0
                    .remove(expiration)
                    .expect("expiration should exist")
                    .1,
            );
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_by_expirations() {
        let mut by_expirations = ByExpiration::default();

        let now = Utc::now();
        let later = now + Duration::seconds(1);
        let sooner = now - Duration::seconds(1);

        let a = RemoteRef::for_slice(&[0x01; 100]);
        let b = RemoteRef::for_slice(&[0x02; 100]);
        let c = RemoteRef::for_slice(&[0x03; 100]);
        let d = RemoteRef::for_slice(&[0x04; 100]);
        let e = RemoteRef::for_slice(&[0x05; 100]);

        by_expirations.add_non_existing(later, a.clone());
        by_expirations.add_non_existing(later, b.clone());
        by_expirations.add_non_existing(now, c.clone());
        by_expirations.add_non_existing(now, d.clone());
        by_expirations.add_non_existing(sooner, e.clone());

        // This should remove e & c.
        //
        // c expires before a & b, and d was added later than c, and so is guaranteed to be removed
        // later even though they have the same expiration.
        // e expires first so it will be removed.
        let mut removed_refs = by_expirations.reclaim(150);

        // The order of returned elements is non-deterministic for efficiency reasons but let's
        // make it so for testing stability.
        removed_refs.sort();

        assert_eq!(&removed_refs, &[c, e]);

        let mut removed_refs = by_expirations.reclaim(50);
        removed_refs.sort();
        assert_eq!(&removed_refs, &[d]);

        // Make sure a is the last kept element.
        by_expirations.replace_existing(later, later, &a);

        let mut removed_refs = by_expirations.reclaim(50);
        removed_refs.sort();
        assert_eq!(&removed_refs, &[b]);

        assert_eq!(by_expirations.remove_existing(later, &a), a);

        assert!(by_expirations.0.is_empty());
    }
}
