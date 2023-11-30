use clap::{Args, Parser, Subcommand};

use georocket::indexer::MainIndexer;
use georocket::{
    importer::GeoDataImporter,
    input::{GeoJsonSplitter, Splitter, SplitterChannels},
    output::{FileStore, Store},
};
use std::{ffi::OsStr, path::PathBuf};

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

#[derive(Args, Debug)]
struct ImportArgs {
    /// the file
    file: PathBuf,
    /// the destination
    path: Option<PathBuf>,
    /// the type of the supplied file
    file_type: Option<String>,
}

impl ImportArgs {
    async fn build(self) -> anyhow::Result<GeoDataImporter> {
        let (channels, chunk_rec, raw_rec) = SplitterChannels::new_with_channels(1024, 1024);
        let splitter = {
            let file = tokio::fs::File::open(self.file).await?;
            Box::new(GeoJsonSplitter::new(file, channels))
        };
        let (indexer, index_rec) = MainIndexer::new_with_index_receiver(1024, chunk_rec);
        let store =
            Box::new(FileStore::new(self.path.unwrap_or_default(), raw_rec, index_rec).await?);
        Ok(GeoDataImporter::new(splitter, store, indexer))
    }
}

enum FileType {
    JSON,
}

impl TryInto<FileType> for &OsStr {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<FileType, Self::Error> {
        match self.try_into()? {
            "json" => Ok(FileType::JSON),
            other => Err(anyhow::anyhow!("unknown file extesnions: '{:?}'", self)),
        }
    }
}

async fn run_import(args: ImportArgs) -> anyhow::Result<()> {
    let importer = args.build().await?;
    importer.run().await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Import(import_args) => run_import(import_args).await,
    }
}
