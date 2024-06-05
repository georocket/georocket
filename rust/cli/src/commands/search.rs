use georocket_core::{
    index::{tantivy::TantivyIndex, Index},
    query::dsl::compile_query,
    storage::{rocksdb::RocksDBStore, Store},
};

use anyhow::{Context, Result};
use ariadne::{Color, Label, Report, ReportKind, Source};
use clap::Args;

/// Search the GeoRocket data store
#[derive(Args, Debug)]
pub struct SearchArgs {
    /// The search query
    pub query: String,
}

/// Run the `search` command
pub fn run_search(args: SearchArgs) -> Result<()> {
    // initialize store
    let store = RocksDBStore::new("store")?;

    // initialize index
    let index = TantivyIndex::new("index")?;

    let compiler_result = compile_query(&args.query);
    match compiler_result {
        Ok(query) => {
            let ids = index.search(query)?;

            // TODO remove this
            println!("Found {} chunks", ids.len());
            for id in ids {
                let chunk = store
                    .get(id)?
                    .with_context(|| format!("Unable to find chunk with ID `{id}'"))?;

                // TODO implement merger
                println!("{}", std::str::from_utf8(&chunk).unwrap());
            }
        }
        Err(errs) => {
            for err in errs {
                Report::build(ReportKind::Error, "query", err.span().start)
                    .with_message(err.to_string())
                    .with_label(
                        Label::new(("query", err.span().into_range()))
                            .with_message(err.reason().to_string())
                            .with_color(Color::Red),
                    )
                    .finish()
                    .eprint(("query", Source::from(&args.query)))
                    .unwrap();
            }
        }
    }

    Ok(())
}
