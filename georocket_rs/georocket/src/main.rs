use clap::{Args, Parser, Subcommand};
use georocket::{
    input::{GeoJsonSplitter, SplitterChannels},
    output::FileStore,
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

struct GeoDataImporter {
    splitter: Splitter,
    store: Store,
}

impl GeoDataImporter {
    fn new(splitter: Splitter, store: Store) -> Self {
        Self { splitter, store }
    }

    async fn run(self) -> anyhow::Result<()> {
        let splitter_handle = tokio::spawn(self.splitter.run());
        let store_handle = tokio::spawn(self.store.run());
        Ok(())
    }
}

enum Store {
    FileStore(FileStore),
}

impl Store {
    async fn run(self) -> anyhow::Result<()> {
        match self {
            Store::FileStore(file_store) => file_store.run().await?,
        };
        Ok(())
    }
}

enum Inner<R> {
    GeoJson(GeoJsonSplitter<R>),
}

enum Splitter {
    File(Inner<tokio::fs::File>),
}

impl Splitter {
    async fn run(self) -> anyhow::Result<()> {
        match self {
            Splitter::File(Inner::GeoJson(geo_json_splitter)) => {
                geo_json_splitter.run().await?;
            }
        };
        Ok(())
    }
}

impl<R> Inner<R> {
    fn from_geo_json_splitter(splitter: GeoJsonSplitter<R>) -> Self {
        Self::GeoJson(splitter)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Import(import_args) => {
            let importer = import_args.build().await?;
            importer.run()
        }
    }
}
