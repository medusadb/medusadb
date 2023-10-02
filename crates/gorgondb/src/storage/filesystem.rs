//! A storage that uses the local filesystem.

use std::path::PathBuf;

use hex::ToHex;

use crate::{filesystem::AsyncFileSource, AsyncSource, Filesystem, RemoteRef};

/// A storage that stores value on disk.
#[derive(Debug, Clone)]
pub struct FilesystemStorage {
    filesystem: Filesystem,
    root: PathBuf,
}

impl FilesystemStorage {
    /// Instantiate a new filesystem storage storing its file at the specified location.
    pub fn new(filesystem: Filesystem, root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;

        Ok(Self { filesystem, root })
    }

    /// Retrieve a value on disk.
    pub async fn retrieve(
        &self,
        remote_ref: &RemoteRef,
    ) -> std::io::Result<Option<AsyncFileSource>> {
        let path = self.get_path(remote_ref);

        self.filesystem.load_source(path).await
    }

    /// Store a value on disk.
    ///
    /// If the file already exists, it is assumed to exist and contain the expected value. As such,
    /// the function will return immediately, in success.
    pub async fn store(
        &self,
        remote_ref: &RemoteRef,
        source: impl Into<AsyncSource<'_>>,
    ) -> std::io::Result<()> {
        let path = self.get_path(remote_ref);

        if tokio::fs::try_exists(&path).await.map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!("failed to check if file `{}` exists", path.display()),
            )
        })? {
            return Ok(());
        }

        self.filesystem
            .save_source(path, source)
            .await
            .map_err(Into::into)
    }

    fn get_path(&self, remote_ref: &RemoteRef) -> PathBuf {
        let hex_id = remote_ref.to_vec().encode_hex::<String>();

        // Make sure we split the identifier in folders to avoid ending up with too many files in a
        // given folder, which can sometimes cause issues on some filesystems.

        // Skip the header and go straight for the hash.
        let header_len = (crate::buf_utils::buffer_size_len(remote_ref.ref_size()) * 2) as usize;
        let (_header, rest) = hex_id.split_at(header_len);
        let (first, rest) = rest.split_at(2);
        let (second, rest) = rest.split_at(2);
        let (third, _) = rest.split_at(2);

        self.root.join(first).join(second).join(third).join(hex_id)
    }
}
