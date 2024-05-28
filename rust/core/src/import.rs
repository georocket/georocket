use anyhow::Result;
use quick_xml::{events::Event, Reader};
use tokio::{fs::File, io::BufReader};
use ulid::Ulid;

use crate::{
    index::{
        gml::generic_attribute_indexer::GenericAttributeIndexer, tantivy::TantivyIndex, Index,
        Indexer,
    },
    input::{xml::FirstLevelSplitter, Splitter},
    storage::{rocksdb::RocksDBStore, Store},
    util::window_read::WindowRead,
};

/// Import an XML file
pub async fn import_xml(path: String) -> Result<()> {
    // initialize store
    let mut store = RocksDBStore::new("store")?;

    // initialize index
    let mut index = TantivyIndex::new("index")?;

    // open file to import
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
            let id = Ulid::new();
            store.add(id, r.chunk).await?;

            let indexer_result = generic_attribute_indexer.make_result();
            index.add(id, indexer_result).await?;
        }

        if e == Event::Eof {
            break;
        }

        buf.clear();
    }

    store.commit().await?;
    index.commit().await?;

    Ok(())
}
