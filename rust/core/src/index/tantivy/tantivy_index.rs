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

use super::{translate_query::QueryTranslator, Fields};

/// An implementation of the [`Index`] trait backed by Tantivy
pub struct TantivyIndex {
    fields: Fields,
    index: tantivy::Index,
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
        let gen_attrs_field = schema_builder.add_json_field("gen_attrs", TEXT);

        let all_values_field = schema_builder.add_text_field("all_values", TEXT);

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
            all_values_field,
        };

        Ok(TantivyIndex {
            fields,
            index,
            reader,
            writer,
        })
    }
}

impl Index for TantivyIndex {
    fn add(&self, id: Ulid, indexer_result: Vec<IndexedValue>) -> anyhow::Result<()> {
        let mut doc = TantivyDocument::new();
        doc.add_bytes(self.fields.id_field, &id.0.to_be_bytes());

        let mut all_values = String::new();
        let mut gen_attrs = BTreeMap::new();

        fn push_all_values(all_values: &mut String, value: &str) {
            if !all_values.is_empty() {
                all_values.push(' ');
            }
            all_values.push_str(value);
        }

        for r in indexer_result {
            match r {
                IndexedValue::GenericAttributes(m) => {
                    for (k, v) in m {
                        match v {
                            crate::index::Value::String(s) => {
                                push_all_values(&mut all_values, &s);
                                gen_attrs.insert(k, OwnedValue::Str(s));
                            }
                            crate::index::Value::Float(f) => {
                                push_all_values(&mut all_values, &f.to_string());
                                gen_attrs.insert(k, OwnedValue::F64(f));
                            }
                            crate::index::Value::Integer(i) => {
                                push_all_values(&mut all_values, &i.to_string());
                                gen_attrs.insert(k, OwnedValue::I64(i));
                            }
                        }
                    }
                }
            }
        }

        doc.add_object(self.fields.gen_attrs_field, gen_attrs);
        doc.add_text(self.fields.all_values_field, all_values);

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

        let query_translator = QueryTranslator::new(&self.index, &self.fields);
        let tantivy_query = query_translator.translate(query)?;

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
    use assertor::{assert_that, VecAssertion};
    use tempdir::TempDir;
    use ulid::Ulid;

    use crate::{
        index::{Index, IndexedValue, Value},
        query::query,
    };

    use super::TantivyIndex;

    struct MiniIndex {
        index: TantivyIndex,
        id1: Ulid,
        id2: Ulid,
        id3: Ulid,
    }

    fn make_mini_index() -> MiniIndex {
        let dir = TempDir::new("georocket_tantivy").unwrap();
        let mut index = TantivyIndex::new(dir.path().to_str().unwrap()).unwrap();

        let id1 = Ulid::new();
        let indexer_result1 = vec![IndexedValue::GenericAttributes(
            [("name".to_string(), Value::String("Elvis".to_string()))].into(),
        )];
        let id2 = Ulid::new();
        let indexer_result2 = vec![IndexedValue::GenericAttributes(
            [(
                "name".to_string(),
                Value::String("Empire State Building".to_string()),
            )]
            .into(),
        )];
        let id3 = Ulid::new();
        let indexer_result3 = vec![IndexedValue::GenericAttributes(
            [("name".to_string(), Value::String("Einar".to_string()))].into(),
        )];

        index.add(id1, indexer_result1).unwrap();
        index.add(id2, indexer_result2).unwrap();
        index.add(id3, indexer_result3).unwrap();
        index.commit().unwrap();

        MiniIndex {
            index,
            id1,
            id2,
            id3,
        }
    }

    #[test]
    fn get_all() {
        let mi = make_mini_index();
        let retrieved_ids = mi.index.search(query![]).unwrap();
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3]);
    }

    #[test]
    fn match_nothing() {
        let mi = make_mini_index();
        let retrieved_ids = mi.index.search(query!["Max"]).unwrap();
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn full_text_single_term() {
        let mi = make_mini_index();
        let retrieved_ids = mi.index.search(query!["empire"]).unwrap();
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);
    }

    #[test]
    fn full_text_two_terms() {
        let mi = make_mini_index();
        let retrieved_ids = mi.index.search(query!["empire", "einar"]).unwrap();
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id3]);
    }

    #[test]
    fn full_text_phrase() {
        let mi = make_mini_index();

        let retrieved_ids = mi.index.search(query!["empire state"]).unwrap();
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = mi.index.search(query!["empire state building"]).unwrap();
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = mi.index.search(query!["empire state build"]).unwrap();
        assert_that!(retrieved_ids).is_empty();
    }
}
