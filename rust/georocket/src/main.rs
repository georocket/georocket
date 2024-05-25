use clap::{Parser, Subcommand};
use commands::import::{run_import, ImportArgs};

mod commands;
mod input;
mod storage;
mod util;

#[derive(Parser, Debug)]
#[command(author, version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Import(ImportArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Import(args) => run_import(args).await,
    }
}
