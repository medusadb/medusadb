use std::{num::NonZeroUsize, sync::Mutex};

use lru::LruCache;

use crate::BlobId;

/// An indexing cache.
#[derive(Debug)]
pub struct Cache<T> {
    inner: Mutex<LruCache<BlobId, T>>,
}

impl<T: Clone> Cache<T> {
    /// Instantiate a new cache with the specified maximum capacity.
    pub fn new(max_capacity: NonZeroUsize) -> Self {
        let inner = Mutex::new(LruCache::new(max_capacity));

        Self { inner }
    }

    /// Get a value from the cache.
    pub fn get(&self, blob_id: &BlobId) -> Option<T> {
        self.inner.lock().unwrap().get(blob_id).cloned()
    }

    /// Set a value in the cache.
    pub fn set(&mut self, blob_id: BlobId, value: T) {
        self.inner.lock().unwrap().push(blob_id, value);
    }
}
