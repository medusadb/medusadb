use std::path::PathBuf;

use clap::{Parser, Subcommand};
use gorgondb::{gorgon::StoreOptions, Cairn};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Store a value and displays its cairn.
    Store {
        /// The path of the file to store.
        path: PathBuf,
    },

    /// Retrieve a value.
    Retrieve {
        /// The cairn of the value to retrieve.
        cairn: Cairn,

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

            let cairn = gorgon.store_from_file(path, options).await?;

            println!("{cairn}");
        }
        Command::Retrieve { cairn, path } => gorgon.retrieve_to_file(cairn, path).await?,
    }

    Ok(())
}