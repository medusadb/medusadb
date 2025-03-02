//! The `gorgoncli` implementation.

use std::path::PathBuf;

use aws_config::BehaviorVersion;
use chrono::Utc;
use clap::{Parser, Subcommand};
use gorgondb::{
    BlobId, Client, Filesystem, Storage, gorgon::StoreOptions, indexing::FixedSizeIndex,
    storage::AwsStorage,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run raw commands.
    Raw {
        /// The test command to execute.
        #[command(subcommand)]
        command: RawCommand,
    },

    /// Run tests.
    Test {
        /// The test command to execute.
        #[command(subcommand)]
        command: TestCommand,
    },
}

#[derive(Subcommand)]
enum RawCommand {
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

#[derive(Subcommand)]
enum TestCommand {
    /// Test indexing of a lot of values.
    Indexing {
        /// The test command to execute.
        #[command(subcommand)]
        command: TestIndexingCommand,
    },
}

#[derive(Subcommand)]
enum TestIndexingCommand {
    /// Test indexing of a lot of values.
    FixedSize {
        /// The count of keys to insert.
        #[clap(long, default_value_t = 16384)]
        count: u64,

        /// The minimum count used for balancing.
        #[clap(long, default_value_t = 4)]
        min_count: u64,

        /// The maximum count used for balancing.
        #[clap(long, default_value_t = 256)]
        max_count: u64,
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

    let filesystem = Filesystem::default();

    match args.command {
        Command::Raw { command } => {
            let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let mut storage: Storage = AwsStorage::new(
                &sdk_config,
                std::env::var("GORGONDB_AWS_S3_BUCKET_NAME")?,
                std::env::var("GORGONDB_AWS_DYNAMODB_TABLE_NAME")?,
            )
            .into();

            storage
                .cache_mut()
                .set_filesystem_storage(Some(filesystem.new_caching_storage("gorgoncli")?));

            let client = gorgondb::Client::new(storage);

            match command {
                RawCommand::Store { path } => {
                    let options = &StoreOptions::default();

                    let blob_id = client.store_from_file(path, options).await?;

                    println!("{blob_id}");
                }
                RawCommand::Retrieve { blob_id, path } => {
                    client.retrieve_to_file(&blob_id, path).await?;
                }
            }
        }
        Command::Test { command } => match command {
            TestCommand::Indexing { command } => match command {
                TestIndexingCommand::FixedSize {
                    count,
                    min_count,
                    max_count,
                } => {
                    let storage = filesystem.new_storage("test")?;
                    let client = Client::new(storage);
                    let before = Utc::now();

                    let blob_id = client
                        .store(
                            "this is a rather large value that is self-contained",
                            &StoreOptions::default(),
                        )
                        .await?;
                    let blob_id2 = client
                        .store(
                            "this is a new large value that is self-contained",
                            &StoreOptions::default(),
                        )
                        .await?;

                    let tx = client.start_transaction("tx1")?;

                    let mut index = FixedSizeIndex::<u64>::new(tx);

                    index.set_balancing_parameters(min_count, max_count)?;

                    // Insert 1024 values in the tree to trigger actual transaction writing as well as
                    // rebalancing.
                    for i in 0..count {
                        index.insert(&i, blob_id.clone()).await?;
                    }

                    for i in 0..count {
                        index.insert(&i, blob_id2.clone()).await?;
                    }

                    for i in 0..count {
                        index.remove(&i).await?;
                    }

                    let after = Utc::now();
                    let duration = after - before;

                    println!("Execution took {}ms", duration.num_milliseconds());
                }
            },
        },
    }

    Ok(())
}
