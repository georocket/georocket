use core::import::import_xml;

use anyhow::{bail, Result};
use clap::Args;

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
