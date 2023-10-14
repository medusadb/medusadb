//! The `gorgoncli` implementation.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use gorgondb::{gorgon::StoreOptions, storage::AwsStorage, BlobId, Filesystem, Storage};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Store a value and displays its blob id.
    Store {
        /// The path of the file to store.
        path: PathBuf,
    },

    /// Retrieve a value.
    Retrieve {
        /// The blob id of the value to retrieve.
        blob_id: BlobId,

        /// The path of the file to write locally.
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let format = tracing_subscriber::fmt::format();

    tracing_subscriber::fmt()
        .event_format(format)
        .with_ansi(true)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let sdk_config = aws_config::load_from_env().await;
    let filesystem = Filesystem::default();
    let mut storage: Storage = AwsStorage::new(
        &sdk_config,
        std::env::var("GORGONDB_AWS_S3_BUCKET_NAME").unwrap(),
        std::env::var("GORGONDB_AWS_DYNAMODB_TABLE_NAME").unwrap(),
    )
    .into();

    //let storage = Storage::Filesystem(filesystem.new_storage("test")?);

    storage
        .cache_mut()
        .set_filesystem_storage(Some(filesystem.new_caching_storage("gorgoncli")?));

    let gorgon = gorgondb::Client::new(storage);

    match args.command {
        Command::Store { path } => {
            let options = &StoreOptions::default();

            let blob_id = gorgon.store_from_file(path, options).await?;

            println!("{blob_id}");
        }
        Command::Retrieve { blob_id, path } => {
            gorgon.retrieve_to_file(blob_id, path).await?;
        }
    }

    Ok(())
}
