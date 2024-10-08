use clap::{Parser, Subcommand};
use commands::{
    delete::{run_delete, DeleteArgs},
    import::{run_import, ImportArgs},
    search::{run_search, SearchArgs},
};

mod commands;

#[derive(Parser, Debug)]
#[command(author, version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Import(ImportArgs),
    Search(SearchArgs),
    Delete(DeleteArgs),
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Import(args) => run_import(args),
        Commands::Search(args) => run_search(args),
        Commands::Delete(args) => run_delete(args),
    }
}
