use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args;
use georocket_core::{
    index::{tantivy::TantivyIndex, Index},
    query::QueryParser,
    storage::{rocksdb::RocksDBStore, Store},
};
use humantime::format_duration;

use crate::commands::search_error::TryIntoSearchError;

/// Delete from the GeoRocket data store
#[derive(Args, Debug)]
pub struct DeleteArgs {
    /// A search query selecting the chunks to delete
    pub query: String,
}

/// Run the `delete` command
pub fn run_delete(args: DeleteArgs) -> Result<()> {
    // initialize store
    let mut store = RocksDBStore::new("store")?;

    // initialize index
    let mut index = TantivyIndex::new("index")?;

    let search_start = Instant::now();
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
    for meta in index.search(query)? {
        let meta = meta?;
        index.delete(meta.id)?;
        store.delete(meta.id)?;
        found_chunks += 1;
    }

    if found_chunks > 0 {
        index.commit()?;
        store.commit()?;
    }

    eprintln!(
        "Deleted {} chunks in {}",
        found_chunks,
        format_duration(Duration::from_millis(
            search_start.elapsed().as_millis() as u64
        ))
    );

    Ok(())
}
