use anyhow::Result;
use std::{collections::BTreeMap, fs};
use tantivy::{
    directory::MmapDirectory,
    schema::{Field, OwnedValue, Schema, STORED, TEXT},
    IndexBuilder, IndexReader, IndexWriter, TantivyDocument,
};
use ulid::Ulid;

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
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use ulid::Ulid;

    use crate::index::{Index, IndexedValue, Value};

    use super::TantivyIndex;

    #[tokio::test]
    async fn add_and_get() {
        let dir = TempDir::new("georocket_tantivy").unwrap();

        let index = TantivyIndex::new(dir.path().to_str().unwrap()).unwrap();

        let id = Ulid::new();

        let indexer_result = vec![IndexedValue::GenericAttributes(
            [("name".to_string(), Value::String("Elvis".to_string()))].into(),
        )];

        index.add(id, indexer_result).await.unwrap();

        todo!("Query added document");
    }
}
