use anyhow::{Context, Result};
use std::{collections::BTreeMap, fs};
use tantivy::{
    collector::DocSetCollector,
    directory::MmapDirectory,
    schema::{OwnedValue, Schema, STORED, TEXT},
    IndexBuilder, IndexReader, IndexWriter, TantivyDocument,
};
use ulid::Ulid;

use crate::{
    index::{Index, IndexedValue},
    query::Query,
};

use super::{translate_query::translate_query, Fields};

/// An implementation of the [`Index`] trait backed by Tantivy
pub struct TantivyIndex {
    fields: Fields,
    reader: IndexReader,
    writer: IndexWriter,
}

impl TantivyIndex {
    /// Creates a new Tantivy index at the given location
    pub fn new(path: &str) -> Result<TantivyIndex> {
        fs::create_dir_all(path)?;

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_bytes_field("_id", STORED);

        // TODO do we really want to use the TEXT tokenizer?
        let gen_attrs_field = schema_builder.add_json_field("genAttrs", TEXT);

        let schema = schema_builder.build();

        let index_dir = MmapDirectory::open(path)?;

        let index = IndexBuilder::new()
            .schema(schema)
            .open_or_create(index_dir)?;

        let reader = index
            .reader_builder()
            // we reload the reader manually in commit()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        let writer = index.writer(1024 * 1024 * 20)?;

        let fields = Fields {
            id_field,
            gen_attrs_field,
        };

        Ok(TantivyIndex {
            fields,
            reader,
            writer,
        })
    }
}

impl Index for TantivyIndex {
    fn add(&self, id: Ulid, indexer_result: Vec<IndexedValue>) -> anyhow::Result<()> {
        let mut doc = TantivyDocument::new();
        doc.add_bytes(self.fields.id_field, &id.0.to_be_bytes());

        let mut gen_attrs = BTreeMap::new();
        for r in indexer_result {
            match r {
                IndexedValue::GenericAttributes(m) => {
                    for (k, v) in m {
                        match v {
                            crate::index::Value::String(s) => {
                                gen_attrs.insert(k, OwnedValue::Str(s));
                            }
                            crate::index::Value::Float(f) => {
                                gen_attrs.insert(k, OwnedValue::F64(f));
                            }
                            crate::index::Value::Integer(i) => {
                                gen_attrs.insert(k, OwnedValue::I64(i));
                            }
                        }
                    }
                }
            }
        }
        doc.add_object(self.fields.gen_attrs_field, gen_attrs);

        self.writer.add_document(doc)?;

        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    fn search(&self, query: Query) -> Result<Vec<Ulid>> {
        let searcher = self.reader.searcher();

        // TODO lowercase values (apply tokenizer?)
        let tantivy_query = translate_query(query, &self.fields);

        let doc_addresses = searcher.search(&tantivy_query, &DocSetCollector)?;

        // fetch matching documents and collect IDs
        let mut result = Vec::new();
        for doc_address in doc_addresses {
            let doc: TantivyDocument = searcher.doc(doc_address)?;

            let doc_id = doc
                .get_first(self.fields.id_field)
                .context("Unable to retrieve ID field from document")?;
            let doc_id_bytes = doc_id
                .as_bytes()
                .context("ID field does not contain bytes")?;
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&doc_id_bytes[..16]);
            let id = Ulid::from_bytes(bytes);

            result.push(id);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use ulid::Ulid;

    use crate::{
        index::{Index, IndexedValue, Value},
        query::query,
    };

    use super::TantivyIndex;

    #[test]
    fn add_and_get() {
        let dir = TempDir::new("georocket_tantivy").unwrap();

        let mut index = TantivyIndex::new(dir.path().to_str().unwrap()).unwrap();

        let id = Ulid::new();

        let indexer_result = vec![IndexedValue::GenericAttributes(
            [("name".to_string(), Value::String("Elvis".to_string()))].into(),
        )];

        index.add(id, indexer_result).unwrap();
        index.commit().unwrap();

        let retrieved_ids = index.search(query![]).unwrap();

        assert_eq!(vec![id], retrieved_ids);
    }
}
