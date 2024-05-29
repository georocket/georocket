use core::{
    index::{tantivy::TantivyIndex, Index},
    query::dsl::compile_query,
    storage::{rocksdb::RocksDBStore, Store},
};

use anyhow::{Context, Result};
use clap::Args;

/// Search the GeoRocket data store
#[derive(Args, Debug)]
pub struct SearchArgs {
    /// The search query
    pub query: String,
}

/// Run the `search` command
pub async fn run_search(args: SearchArgs) -> Result<()> {
    // initialize store
    let store = RocksDBStore::new("store")?;

    // initialize index
    let index = TantivyIndex::new("index")?;

    let compiler_result = compile_query(&args.query);
    match compiler_result {
        Ok(query) => {
            let ids = index.search(query).await?;

            // TODO remove this
            println!("Found {} chunks", ids.len());
            for id in ids {
                let chunk = store
                    .get(id)
                    .await?
                    .with_context(|| format!("Unable to find chunk with ID `{id}'"))?;

                // TODO implement merger
                println!("{}", std::str::from_utf8(&chunk).unwrap());
            }
        }
        Err(err) => {
            eprintln!("{err:?}");
            todo!();
        }
    }

    Ok(())
}
