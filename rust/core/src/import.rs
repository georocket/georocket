use std::{fs::File, io::BufReader, sync::mpsc, thread::spawn};

use anyhow::{bail, Ok, Result};
use quick_xml::{events::Event, Reader};
use ulid::Ulid;

use crate::{
    index::{
        gml::{generic_attribute_indexer::GenericAttributeIndexer, srs_indexer::SRSIndexer},
        tantivy::TantivyIndex,
        Index, IndexedValue, Indexer,
    },
    input::{xml::FirstLevelSplitter, Splitter},
    storage::{rocksdb::RocksDBStore, Store},
    util::window_read::WindowRead,
};

/// Import an XML file
pub fn import_xml(path: String) -> Result<()> {
    // initialize store
    let mut store = RocksDBStore::new("store")?;

    // initialize index
    let mut index = TantivyIndex::new("index")?;

    // run separate thread for store
    let (store_send, store_recv) = mpsc::sync_channel::<(Ulid, Vec<u8>)>(16);
    let store_thread = spawn(move || {
        for (id, chunk) in store_recv {
            store.add(id, chunk)?;
        }
        store.commit()?;
        Ok(())
    });

    // run separate thread for index
    let (index_send, index_recv) = mpsc::sync_channel::<(Ulid, Vec<IndexedValue>)>(16);
    let index_thread = spawn(move || {
        for (id, indexer_result) in index_recv {
            index.add(id, indexer_result)?;
        }
        index.commit()?;
        Ok(())
    });

    // open file to import
    let file = File::open(path)?;
    let window = WindowRead::new(file);

    let bufreader = BufReader::new(window);
    let mut reader = Reader::from_reader(bufreader);

    let mut srs_indexer = SRSIndexer::default();
    let mut generic_attribute_indexer = GenericAttributeIndexer::default();

    let mut buf = Vec::new();
    let mut splitter = FirstLevelSplitter::default();
    loop {
        let start_pos = reader.buffer_position();
        let e = reader.read_event_into(&mut buf)?;
        let end_pos = reader.buffer_position();
        let window = reader.get_mut().get_mut().window_mut();

        // SRSIndexer must be called before all other indexers because it needs
        // to record the current SRS. Don't call it when the event represents
        // and end tag. The current SRS should stay in effect until all
        // indexers have processed the end tag.
        if !matches!(e, Event::End(_)) {
            srs_indexer.on_event(&e)?;
        }

        generic_attribute_indexer.on_event(&e)?;

        // now call SRSIndexer also for the end tag
        if matches!(e, Event::End(_)) {
            srs_indexer.on_event(&e)?;
        }

        if let Some(r) = splitter.on_event(&e, start_pos..end_pos, window)? {
            let id = Ulid::new();
            store_send.send((id, r.chunk))?;

            let indexer_result = generic_attribute_indexer.into();
            index_send.send((id, indexer_result))?;

            // reset indexers
            generic_attribute_indexer = GenericAttributeIndexer::default();
        }

        if e == Event::Eof {
            break;
        }

        buf.clear();
    }

    drop(store_send);
    if let Err(err) = store_thread.join() {
        bail!("Store thread threw an error: {err:?}");
    }

    drop(index_send);
    if let Err(err) = index_thread.join() {
        bail!("Index thread threw an error: {err:?}");
    }

    Ok(())
}
