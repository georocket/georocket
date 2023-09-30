use clap::{Args, Parser, Subcommand};

use georocket::{
    importer::GeoDataImporter,
    input::{GeoJsonSplitter, Inner, Splitter, SplitterChannels},
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
}

impl ImportArgs {
    async fn build(self) -> anyhow::Result<GeoDataImporter> {
        let ImportArgs { file, path } = self;

        if !file.is_file() {
            if file.is_dir() {
                anyhow::bail!(
                    "specified path for input file is a directory, not a file: '{:?}'",
                    file
                );
            } else {
                anyhow::bail!("")
            }
        }

        let filetype: FileType = if let Some(extension) = file.as_path().extension() {
            extension.try_into()?
        } else {
            anyhow::bail!(
                "cannot infer file type from filename: '{:?}'",
                file.as_path().extension()
            );
        };
        let file = tokio::fs::File::open(file).await?;

        let (splitter_channels, chunk_rec, raw_rec) =
            SplitterChannels::new_with_channels(1024, 1024);

        let splitter = match filetype {
            FileType::JSON => Splitter::File(Inner::GeoJson(GeoJsonSplitter::new(
                file,
                splitter_channels,
            ))),
        };

        let destination = if let Some(destination) = path {
            destination
        } else {
            std::env::current_dir()?
        };

        let store = Store::FileStore(FileStore::new(destination, raw_rec).await?);
        Ok(GeoDataImporter::new(splitter, store))
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    Ok(match cli.command {
        Commands::Import(import_args) => {
            let importer = import_args.build().await?;
            importer.run().await?
        }
    })
}
