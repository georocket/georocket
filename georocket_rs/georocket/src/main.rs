use clap::{Parser, Subcommand};
use georocket::importer::import_args::{run_import, ImportArgs};

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
        Commands::Import(import_args) => run_import(import_args).await,
    }
}
