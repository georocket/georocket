use anyhow::{bail, Result};
use clap::Args;
use quick_xml::{events::Event, Reader};
use tokio::{fs::File, io::BufReader};

use crate::{
    input::{xml::FirstLevelSplitter, Splitter},
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
    let file = File::open(path).await?;
    let window = WindowRead::new(file);
    let bufreader = BufReader::new(window);
    let mut reader = Reader::from_reader(bufreader);

    let mut buf = Vec::new();
    let mut splitter = FirstLevelSplitter::default();
    let mut count = 0;
    loop {
        let start_pos = reader.buffer_position();
        let e = reader.read_event_into_async(&mut buf).await?;
        let end_pos = reader.buffer_position();
        let window = reader.get_mut().get_mut().window_mut();
        if let Some(r) = splitter.on_event(&e, start_pos..end_pos, window)? {
            // todo
            // println!("---------");
            // println!("{}", String::from_utf8(r.chunk).unwrap());
            // println!("---------");
            count += 1;
        }
        if e == Event::Eof {
            println!("{}", count);
            break;
        }
        buf.clear();
    }

    Ok(())
}
