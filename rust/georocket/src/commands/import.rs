use anyhow::{bail, Result};
use clap::Args;
use quick_xml::{events::Event, Reader};
use tokio::{fs::File, io::BufReader};
use ulid::Ulid;

use crate::{
    index::{
        gml::generic_attribute_indexer::{self, GenericAttributeIndexer},
        Indexer,
    },
    input::{xml::FirstLevelSplitter, Splitter},
    storage::{rocksdb::RocksDBStore, Store},
    util::window_read::WindowRead,
};

/// Import one or more files into a GeoRocket store
#[derive(Args, Debug)]
pub struct ImportArgs {
    /// One or more files to import
    #[arg(name = "FILE")]
    pub(super) files: Vec<String>,

    /// The path or URI of the store to import into
    #[arg(long, short)]
    pub(super) destination: Option<String>,
}

/// The type of a file to import
enum FileType {
    Xml,
    Json,
}

/// Run the `import` command
pub async fn run_import(args: ImportArgs) -> Result<()> {
    let mut files_with_types = Vec::new();

    // detect file types
    for path in args.files {
        let mime = mime_guess::from_path(&path);
        match mime.first() {
            Some(t) => {
                let ft = match t.subtype().as_str() {
                    "json" => FileType::Json,
                    "gml" | "xml" => FileType::Xml,
                    _ => bail!("Unsupported file type: `{path}' -> `{t}'"),
                };
                files_with_types.push((path, ft));
            }
            None => bail!("Unable to detect file type: `{path}'"),
        }
    }

    // import all files
    for (path, file_type) in files_with_types {
        match file_type {
            FileType::Xml => import_xml(path).await?,
            FileType::Json => todo!("Importing JSON files is not supported yet"),
        }
    }

    Ok(())
}

/// Import an XML file
async fn import_xml(path: String) -> Result<()> {
    let mut store = RocksDBStore::new("store")?;

    let file = File::open(path).await?;
    let window = WindowRead::new(file);

    // use larger buffer to reduce number of asynchronous I/O calls
    let bufreader = BufReader::with_capacity(1024 * 128, window);
    let mut reader = Reader::from_reader(bufreader);

    let mut generic_attribute_indexer = GenericAttributeIndexer::default();

    let mut buf = Vec::new();
    let mut splitter = FirstLevelSplitter::default();
    loop {
        let start_pos = reader.buffer_position();
        let e = reader.read_event_into_async(&mut buf).await?;
        let end_pos = reader.buffer_position();
        let window = reader.get_mut().get_mut().window_mut();

        generic_attribute_indexer.on_event(&e)?;

        if let Some(r) = splitter.on_event(&e, start_pos..end_pos, window)? {
            store.add(Ulid::new(), r.chunk).await?;

            let indexer_result = generic_attribute_indexer.make_result();
        }

        if e == Event::Eof {
            break;
        }

        buf.clear();
    }

    store.commit().await?;

    Ok(())
}
