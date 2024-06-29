use std::{fs::File, io::BufReader, sync::Arc, thread::spawn};

use anyhow::{bail, Ok, Result};
use crossbeam_channel::bounded;
use parking_lot::RwLock;
use quick_xml::{events::Event, Reader};
use ulid::Ulid;

use crate::{
    index::{
        chunk_meta::ChunkMeta,
        gml::{
            bounding_box_indexer::BoundingBoxIndexer,
            generic_attribute_indexer::GenericAttributeIndexer, root_element::RootElement,
            srs_indexer::SRSIndexer,
        },
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
    let index = Arc::new(RwLock::new(TantivyIndex::new("index")?));

    // run separate thread for store
    let (store_send, store_recv) = bounded::<(Ulid, Vec<u8>)>(16);
    let store_thread = spawn(move || {
        for (id, chunk) in store_recv {
            store.add(id, chunk)?;
        }
        store.commit()?;
        Ok(())
    });

    // Run separate threads for index. Even though the underlying indexer
    // might not be multi-threaded or might have its own threading layer,
    // preparing the documents (e.g. creating spatial indexing terms) will
    // benefit from parallelization.
    let (index_send, index_recv) = bounded::<(ChunkMeta, Vec<IndexedValue>)>(16);
    let index_threads = (0..num_cpus::get())
        .map(|_| {
            let index = Arc::clone(&index);
            let index_recv = index_recv.clone();
            spawn(move || {
                let index = index.read();
                for (meta, indexer_result) in index_recv {
                    index.add(meta, indexer_result)?;
                }
                Ok(())
            })
        })
        .collect::<Vec<_>>();

    // open file to import
    let file = File::open(path)?;
    let window = WindowRead::new(file);

    let bufreader = BufReader::new(window);
    let mut reader = Reader::from_reader(bufreader);

    let mut srs_indexer = SRSIndexer::default();
    let mut bounding_box_indexer = BoundingBoxIndexer::default();
    let mut generic_attribute_indexer = GenericAttributeIndexer::default();

    let mut buf = Vec::new();
    let mut splitter = FirstLevelSplitter::default();
    let mut root_element: Option<RootElement> = None;
    loop {
        let start_pos = reader.buffer_position();
        let e = reader.read_event_into(&mut buf)?;
        let end_pos = reader.buffer_position();
        let window = reader.get_mut().get_mut().window_mut();

        // SRSIndexer must be called before all other indexers because it needs
        // to record the current SRS. Don't call it when the event represents
        // an end tag. The current SRS should stay in effect until all
        // indexers have processed the end tag.
        if !matches!(e, Event::End(_)) {
            srs_indexer.on_event(&e)?;
        }

        bounding_box_indexer.on_event((&e, srs_indexer.context()))?;
        generic_attribute_indexer.on_event(&e)?;

        // now call SRSIndexer also for the end tag
        if matches!(e, Event::End(_)) {
            srs_indexer.on_event(&e)?;
        }

        if let Some(r) = splitter.on_event(&e, start_pos..end_pos, window)? {
            let id = Ulid::new();
            store_send.send((id, r))?;

            if root_element.is_none() {
                root_element = Some(RootElement::try_from_xml_tag(
                    splitter.root().unwrap(),
                    &reader,
                )?);
            }

            let meta = ChunkMeta {
                id,
                root_element: root_element.clone().unwrap(),
            };

            let mut indexer_result: Vec<_> = generic_attribute_indexer.into();
            if let Some(v) = bounding_box_indexer.try_into()? {
                indexer_result.push(v);
            }

            index_send.send((meta, indexer_result))?;

            // reset indexers
            bounding_box_indexer = BoundingBoxIndexer::default();
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
    for index_thread in index_threads {
        if let Err(err) = index_thread.join() {
            bail!("Index thread threw an error: {err:?}");
        }
    }
    index.write().commit()?;

    Ok(())
}
