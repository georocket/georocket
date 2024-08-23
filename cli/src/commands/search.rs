use std::{
    io::{self, BufWriter},
    thread::spawn,
    time::{Duration, Instant},
};

use crossbeam_channel::bounded;
use georocket_core::{
    index::{tantivy::TantivyIndex, Index},
    output::{xml::XmlMerger, Merger},
    query::QueryParser,
    storage::{rocksdb::RocksDBStore, Store},
};

use anyhow::{bail, Context, Result};
use clap::Args;
use humantime::format_duration;

use super::search_error::TryIntoSearchError;

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

    // initialize merger
    let stdout = io::stdout().lock();
    let writer = BufWriter::new(stdout);
    let mut merger = XmlMerger::new(writer);

    let search_start = Instant::now();
    let mut first = false;
    let mut found_chunks = 0;

    // parse query
    let query_parser = QueryParser::new();
    let query = match query_parser.parse(&args.query) {
        Ok(query) => Ok(query),
        Err(err) => {
            let se = err.try_into_search_error(&args.query)?;
            Err(se)
        }
    }?;

    // perform search
    let (search_sender, search_receiver) = bounded(1024 * 10);

    let search_thread = spawn(move || {
        for meta in index.search(query)? {
            let meta = meta?;
            search_sender.send(meta)?;
        }
        anyhow::Ok(())
    });

    for meta in search_receiver {
        // TODO remove this
        if !first {
            eprintln!("Found first chunk after {:?}", search_start.elapsed());
            first = true;
        }

        let chunk = store
            .get(meta.id)?
            .with_context(|| format!("Unable to find chunk with ID `{}'", meta.id))?;

        merger.merge(&chunk, meta)?;

        found_chunks += 1;
    }

    if let Err(err) = search_thread.join() {
        bail!("Search thread threw an error: {err:?}");
    }

    merger.finish()?;

    eprintln!(
        "Found {} chunks in {}",
        found_chunks,
        format_duration(Duration::from_millis(
            search_start.elapsed().as_millis() as u64
        ))
    );

    Ok(())
}
