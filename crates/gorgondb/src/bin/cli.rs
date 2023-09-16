//! The `gorgoncli` implementation.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use gorgondb::{gorgon::StoreOptions, BlobId};

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

    let gorgon = gorgondb::Gorgon::default();

    match args.command {
        Command::Store { path } => {
            let options = &StoreOptions::default();

            let blob_id = gorgon.store_from_file(path, options).await?;

            println!("{blob_id}");
        }
        Command::Retrieve { blob_id, path } => gorgon.retrieve_to_file(blob_id, path).await?,
    }

    Ok(())
}
