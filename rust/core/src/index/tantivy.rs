use anyhow::{Context, Result};
use std::{collections::BTreeMap, fs};
use tantivy::{
    collector::DocSetCollector,
    directory::MmapDirectory,
    query::AllQuery,
    schema::{Field, OwnedValue, Schema, Value, STORED, TEXT},
    IndexBuilder, IndexReader, IndexWriter, TantivyDocument,
};
use ulid::Ulid;

use crate::query::Query;

use super::{Index, IndexedValue};

/// An implementation of the [`Index`] trait backed by Tantivy
pub struct TantivyIndex {
    id_field: Field,
    gen_attrs_field: Field,

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

        // TODO having both reader and writer might not be necessary in all cases
        let reader = index
            .reader_builder()
            // we reload the reader manually in commit()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        let writer = index.writer(1024 * 1024 * 20)?;

        Ok(TantivyIndex {
            id_field,
            gen_attrs_field,
            reader,
            writer,
        })
    }
}

impl Index for TantivyIndex {
    async fn add(&self, id: Ulid, indexer_result: Vec<IndexedValue>) -> anyhow::Result<()> {
        let mut doc = TantivyDocument::new();
        doc.add_bytes(self.id_field, id.0.to_be_bytes());

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
        doc.add_object(self.gen_attrs_field, gen_attrs);

        self.writer.add_document(doc)?;

        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    async fn search(&self, query: Query) -> Result<Vec<Ulid>> {
        let searcher = self.reader.searcher();

        let tantivy_query = if query.components.is_empty() {
            AllQuery
        } else {
            todo!()
        };
        let doc_addresses = searcher.search(&tantivy_query, &DocSetCollector)?;

        // fetch matching documents and collect IDs
        let mut result = Vec::new();
        for doc_address in doc_addresses {
            let doc: TantivyDocument = searcher.doc(doc_address)?;

            let doc_id = doc
                .get_first(self.id_field)
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

    #[tokio::test]
    async fn add_and_get() {
        let dir = TempDir::new("georocket_tantivy").unwrap();

        let mut index = TantivyIndex::new(dir.path().to_str().unwrap()).unwrap();

        let id = Ulid::new();

        let indexer_result = vec![IndexedValue::GenericAttributes(
            [("name".to_string(), Value::String("Elvis".to_string()))].into(),
        )];

        index.add(id, indexer_result).await.unwrap();
        index.commit().await.unwrap();

        let retrieved_ids = index.search(query![]).await.unwrap();

        assert_eq!(vec![id], retrieved_ids);
    }
}
