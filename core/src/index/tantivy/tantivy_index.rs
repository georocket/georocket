use anyhow::{Context, Result};
use h3o::Resolution;
use std::{collections::BTreeMap, fs, rc::Rc};
use tantivy::{
    directory::MmapDirectory,
    query::{EnableScoring, Scorer, Weight},
    schema::{OwnedValue, Schema, FAST, INDEXED, STORED, STRING, TEXT},
    DocAddress, DocId, IndexBuilder, IndexReader, IndexWriter, Searcher, TantivyDocument,
    COLLECT_BLOCK_BUFFER_LEN,
};
use ulid::Ulid;

use crate::{
    index::{
        h3_term_index::{make_terms, TermMode, TermOptions},
        Index, IndexedValue, Value,
    },
    query::Query,
};

use super::{query_translator::QueryTranslator, Fields};

/// An implementation of the [`Index`] trait backed by Tantivy
pub struct TantivyIndex {
    fields: Fields,
    index: tantivy::Index,
    reader: IndexReader,
    writer: IndexWriter,
    bbox_term_options: TermOptions,
}

impl TantivyIndex {
    /// Creates a new Tantivy index at the given location
    pub fn new(path: &str) -> Result<TantivyIndex> {
        fs::create_dir_all(path)?;

        let mut schema_builder = Schema::builder();

        let id_field = schema_builder.add_bytes_field("_id", STORED | INDEXED);
        let gen_attrs_field = schema_builder.add_json_field("gen_attrs", STRING);
        let all_values_field = schema_builder.add_text_field("all_values", TEXT);
        let bbox_min_x_field = schema_builder.add_f64_field("bbox_min_x", FAST);
        let bbox_min_y_field = schema_builder.add_f64_field("bbox_min_y", FAST);
        let bbox_max_x_field = schema_builder.add_f64_field("bbox_max_x", FAST);
        let bbox_max_y_field = schema_builder.add_f64_field("bbox_max_y", FAST);
        let bbox_terms_field = schema_builder.add_text_field("bbox_terms", STRING);

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
            bbox_min_x_field,
            bbox_min_y_field,
            bbox_max_x_field,
            bbox_max_y_field,
            bbox_terms_field,
        };

        // TODO If necessary, this could be made configurable. Note that
        // changing the term options here requires the whole index to be
        // rebuilt, as they must be the same for both indexing and querying. If
        // we make this configurable, the options should be stored somewhere in
        // the index (or metadata), so we can compare them when opening it.
        let bbox_term_options = TermOptions {
            min_resolution: Resolution::One,
            max_resolution: Resolution::Fifteen,
            max_cells: 8,
            optimize_for_space: false,
        };

        Ok(TantivyIndex {
            fields,
            index,
            reader,
            writer,
            bbox_term_options,
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
                            Value::String(s) => {
                                push_all_values(&mut all_values, &s);
                                gen_attrs.insert(k, OwnedValue::Str(s));
                            }
                            Value::Float(f) => {
                                push_all_values(&mut all_values, &f.to_string());
                                gen_attrs.insert(k, OwnedValue::F64(f));
                            }
                            Value::Integer(i) => {
                                push_all_values(&mut all_values, &i.to_string());
                                gen_attrs.insert(k, OwnedValue::I64(i));
                            }
                        }
                    }
                }

                IndexedValue::BoundingBox(bbox) => {
                    doc.add_f64(self.fields.bbox_min_x_field, bbox.min().x);
                    doc.add_f64(self.fields.bbox_min_y_field, bbox.min().y);
                    doc.add_f64(self.fields.bbox_max_x_field, bbox.max().x);
                    doc.add_f64(self.fields.bbox_max_y_field, bbox.max().y);

                    let terms = make_terms(&bbox, self.bbox_term_options, TermMode::Index)?;
                    for t in terms {
                        doc.add_text(self.fields.bbox_terms_field, t);
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

    fn search(&self, query: Query) -> Result<impl Iterator<Item = Result<Ulid>>> {
        let searcher = Rc::new(self.reader.searcher());

        let query_translator =
            QueryTranslator::new(&self.index, &self.fields, self.bbox_term_options);
        let tantivy_query = query_translator.translate(query)?;
        let weight = tantivy_query.weight(EnableScoring::Disabled {
            schema: searcher.schema(),
            searcher_opt: Some(&searcher),
        })?;

        let map_docset_to_doc_addresses = |searcher: Rc<Searcher>| {
            move |(index, docset)| {
                let searcher = searcher.clone();
                DocSetIterator::new(docset)
                    .filter(move |&doc_id| {
                        searcher
                            .segment_reader(index as u32)
                            .alive_bitset()
                            .map(|ab| ab.is_alive(doc_id))
                            .unwrap_or(true)
                    })
                    .map(move |doc_id| DocAddress::new(index as u32, doc_id))
            }
        };

        let map_doc_address_to_chunk_id = |searcher: Rc<Searcher>| {
            move |doc_address| {
                let doc: TantivyDocument = searcher.doc(doc_address)?;

                let id_field_value = doc
                    .get_first(self.fields.id_field)
                    .context("Unable to retrieve ID field from document")?;
                let id_bytes = id_field_value
                    .as_bytes()
                    .context("ID field does not contain bytes")?;

                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&id_bytes[..16]);

                anyhow::Ok(Ulid::from_bytes(bytes))
            }
        };

        // create an iterator that performs the search for us and yields the
        // first chunk ID as soon as possible
        Ok(SearchIterator::new(Rc::clone(&searcher), weight)
            .flatten()
            .flat_map(map_docset_to_doc_addresses(Rc::clone(&searcher)))
            .map(map_doc_address_to_chunk_id(Rc::clone(&searcher))))
    }
}

/// Iterates over all segments in a [`Searcher`], performs a search using
/// the given [`Weight`], and returns a scorer
struct SearchIterator {
    searcher: Rc<Searcher>,
    weight: Box<dyn Weight>,
    index: usize,
}

impl SearchIterator {
    fn new(searcher: Rc<Searcher>, weight: Box<dyn Weight>) -> Self {
        Self {
            searcher,
            weight,
            index: 0,
        }
    }
}

impl Iterator for SearchIterator {
    type Item = Result<(usize, Box<dyn Scorer>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let segment_readers = self.searcher.segment_readers();
        if self.index == segment_readers.len() {
            None
        } else {
            let r = Some(
                self.weight
                    .scorer(&segment_readers[self.index], 1.0)
                    .map(|scorer| (self.index, scorer))
                    .map_err(|e| e.into()),
            );
            self.index += 1;
            r
        }
    }
}

/// Iterates over the doc sets returned by a scorer and returns doc addresses
struct DocSetIterator {
    docset: Box<dyn Scorer>,
    buffer: [u32; COLLECT_BLOCK_BUFFER_LEN],
    finished: bool,
    num_items: usize,
    index: usize,
}

impl DocSetIterator {
    fn new(docset: Box<dyn Scorer>) -> Self {
        Self {
            docset,
            buffer: [0u32; COLLECT_BLOCK_BUFFER_LEN],
            finished: false,
            num_items: 0,
            index: 0,
        }
    }
}

impl Iterator for DocSetIterator {
    type Item = DocId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.num_items {
            if self.finished {
                return None;
            }

            self.num_items = self.docset.fill_buffer(&mut self.buffer);
            self.index = 0;

            if self.num_items < self.buffer.len() {
                self.finished = true;
            }

            if self.num_items == 0 {
                return None;
            }
        }

        let doc_id = self.buffer[self.index];
        self.index += 1;

        Some(doc_id)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use assertor::{assert_that, BooleanAssertion, EqualityAssertion, VecAssertion};
    use geo::{coord, Rect};
    use tantivy::{Term, COLLECT_BLOCK_BUFFER_LEN};
    use tempdir::TempDir;
    use ulid::Ulid;

    use crate::{
        index::{Index, IndexedValue, Value},
        query::{and, bbox, eq, gt, gte, lt, lte, not, or, query, Query},
    };

    use super::TantivyIndex;

    struct MiniIndex {
        _dir: TempDir,
        index: TantivyIndex,
        id1: Ulid,
        id2: Ulid,
        id3: Ulid,
        id4: Ulid,
    }

    struct LargeIndex {
        _dir: TempDir,
        index: TantivyIndex,
    }

    fn make_mini_index() -> MiniIndex {
        let dir = TempDir::new("georocket_tantivy").unwrap();
        let mut index = TantivyIndex::new(dir.path().to_str().unwrap()).unwrap();

        let id1 = Ulid::new();
        let indexer_result1 = vec![IndexedValue::GenericAttributes(
            [("name".to_string(), Value::String("Elvis".to_string()))].into(),
        )];
        let id2 = Ulid::new();
        let indexer_result2 = vec![
            IndexedValue::GenericAttributes(
                [
                    (
                        "name".to_string(),
                        Value::String("Empire State Building".to_string()),
                    ),
                    ("year".to_string(), Value::Integer(1931)),
                    ("height".to_string(), Value::Float(443.2)),
                ]
                .into(),
            ),
            IndexedValue::BoundingBox(Rect::new(
                // aprox. :-)
                coord! { x: -73.986479, y: 40.747914 },
                coord! { x: -73.984866, y: 40.748978 },
            )),
        ];
        let id3 = Ulid::new();
        let indexer_result3 = vec![IndexedValue::GenericAttributes(
            [("name".to_string(), Value::String("Einar".to_string()))].into(),
        )];
        let id4 = Ulid::new();
        let indexer_result4 = vec![
            IndexedValue::GenericAttributes(
                [
                    ("name".to_string(), Value::String("Main Tower".to_string())),
                    ("year".to_string(), Value::Integer(2000)),
                    ("height".to_string(), Value::Float(240.0)),
                ]
                .into(),
            ),
            IndexedValue::BoundingBox(Rect::new(
                // aprox. :-)
                coord! { x: 8.6718699, y: 50.1123123 },
                coord! { x: 8.6725155, y: 50.1127384 },
            )),
        ];

        index.add(id1, indexer_result1).unwrap();
        index.add(id2, indexer_result2).unwrap();
        index.add(id3, indexer_result3).unwrap();
        index.add(id4, indexer_result4).unwrap();
        index.commit().unwrap();

        MiniIndex {
            _dir: dir,
            index,
            id1,
            id2,
            id3,
            id4,
        }
    }

    fn make_large_index(n_items: usize) -> LargeIndex {
        let dir = TempDir::new("georocket_tantivy").unwrap();
        let mut index = TantivyIndex::new(dir.path().to_str().unwrap()).unwrap();

        for _ in 0..n_items {
            let id = Ulid::new();
            let indexer_result = vec![IndexedValue::GenericAttributes(
                [("id".to_string(), Value::String(id.to_string()))].into(),
            )];
            index.add(id, indexer_result).unwrap();
        }

        index.commit().unwrap();

        LargeIndex { _dir: dir, index }
    }

    fn search(mi: &MiniIndex, query: Query) -> Vec<Ulid> {
        mi.index
            .search(query)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap()
    }

    fn search_large(li: &LargeIndex, query: Query) -> Vec<Ulid> {
        li.index
            .search(query)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap()
    }

    #[test]
    fn get_all() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3, mi.id4]);
    }

    /// Make sure the search result does not contain deleted chunks
    #[test]
    fn delete() {
        let mut mi = make_mini_index();
        assert_that!(mi.index.reader.searcher().num_docs()).is_equal_to(4);

        // TODO use delete method of TantivyIndex once we've implemented it
        mi.index.writer.delete_term(Term::from_field_bytes(
            mi.index.fields.id_field,
            &mi.id2.0.to_be_bytes(),
        ));
        mi.index.commit().unwrap();

        assert_that!(mi.index.reader.searcher().num_docs()).is_equal_to(3);
        assert_that!(mi.index.reader.searcher().segment_reader(0).has_deletes()).is_true();
        assert_that!(mi
            .index
            .reader
            .searcher()
            .segment_reader(0)
            .num_deleted_docs())
        .is_equal_to(1);

        let retrieved_ids = search(&mi, query![]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id3, mi.id4]);
    }

    #[test]
    fn match_nothing() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query!["Max"]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn full_text_single_term() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query!["empire"]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);
    }

    #[test]
    fn full_text_two_terms() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![or!["empire", "einar"]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id3]);
    }

    #[test]
    fn full_text_phrase() {
        let mi = make_mini_index();

        let retrieved_ids = search(&mi, query!["empire state"]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query!["empire state building"]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query!["empire state build"]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn eq_string() {
        let mi = make_mini_index();

        let retrieved_ids = search(&mi, query![eq!["name", "Empire State Building"]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![eq!["name", "Empire State"]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![eq!["name", "empire state building"]]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn eq_integer() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![eq!["year", 1931]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![eq!["year", "1931"]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![eq!["year", 443]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![eq!["year", 1931.0]]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn eq_float() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![eq!["height", 443.2]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![eq!["height", 443]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![eq!["height", "443"]]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn gt() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![gt!["year", 1900]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![gt!["year", 1990]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![gt!["year", 1999]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![gt!["year", 2000]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![gt!["height", 100.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![gt!["height", 300.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![gt!["height", 443.1999]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![gt!["height", 443.2]]);
        assert_that!(retrieved_ids).is_empty();

        // TODO we can't compare f64 with i64, which is correct (i.e. type-safe)
        // but inconvenient for the user
        let retrieved_ids = search(&mi, query![gt!["height", 300]]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn gte() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![gte!["year", 1900]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![gte!["year", 1990]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![gte!["year", 2000]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![gte!["year", 2001]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![gte!["height", 100.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![gte!["height", 300.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![gte!["height", 443.2]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![gte!["height", 443.200001]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![gte!["height", 300]]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn lt() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![lt!["year", 2100]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![lt!["year", 1990]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![lt!["year", 1932]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![lt!["year", 1931]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![lt!["height", 1000.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![lt!["height", 300.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![lt!["height", 240.001]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![lt!["height", 240.0]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![lt!["height", 300]]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn lte() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![lte!["year", 2100]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![lte!["year", 1990]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![lte!["year", 1931]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![lte!["year", 1930]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![lte!["height", 1000.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![lte!["height", 300.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![lte!["height", 240.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![lte!["height", 239.999]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![lte!["height", 300]]);
        assert_that!(retrieved_ids).is_empty();
    }

    #[test]
    fn or() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![or![eq!["year", 1931], eq!["year", 2000]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2, mi.id4]);

        let retrieved_ids = search(&mi, query![or![eq!["year", 1931], eq!["foo", "bar"]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![or![eq!["year", 1900], eq!["foo", "bar"]]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(&mi, query![or![eq!["year", 1931]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![or![eq!["year", 1931], or![]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![or![]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3, mi.id4]);
    }

    #[test]
    fn and() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![and![eq!["year", 1931], eq!["year", 2000]]]);
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(
            &mi,
            query![and![
                eq!["year", 1931],
                eq!["name", "Empire State Building"]
            ]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![and![eq!["year", 1931]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![or![eq!["year", 1931], and![]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![and![]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3, mi.id4]);
    }

    #[test]
    fn not() {
        let mut mi = make_mini_index();
        let retrieved_ids = search(&mi, query![not![or![eq!["year", 1931], eq!["year", 2000]]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id3]);

        let retrieved_ids = search(&mi, query![not![eq!["year", 1931]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id3, mi.id4]);

        let retrieved_ids = search(
            &mi,
            query![not![and![
                eq!["year", 1931],
                eq!["name", "Empire State Building"]
            ]]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id3, mi.id4]);

        let retrieved_ids = search(
            &mi,
            query![not![and![
                eq!["year", 2000],
                eq!["name", "Empire State Building"]
            ]]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3, mi.id4]);

        let retrieved_ids = search(
            &mi,
            query![or![
                eq!["year", 2000],
                not![and![
                    eq!["year", 2000],
                    eq!["name", "Empire State Building"]
                ]]
            ]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3, mi.id4]);

        let retrieved_ids = search(
            &mi,
            query![or![
                eq!["year", 1931],
                not![and![
                    eq!["year", 1931],
                    eq!["name", "Empire State Building"]
                ]]
            ]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3, mi.id4]);

        let retrieved_ids = search(
            &mi,
            query![and![
                eq!["year", 1931],
                not![and![
                    eq!["year", 1931],
                    eq!["name", "Empire State Building"]
                ]]
            ]],
        );
        assert_that!(retrieved_ids).is_empty();

        let retrieved_ids = search(
            &mi,
            query![and![
                eq!["year", 2000],
                not![and![
                    eq!["year", 2000],
                    eq!["name", "Empire State Building"]
                ]]
            ]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let id5 = Ulid::new();
        let indexer_result5 = vec![IndexedValue::GenericAttributes(
            [
                (
                    "name".to_string(),
                    Value::String("Empire State Building".to_string()),
                ),
                ("year".to_string(), Value::Integer(2000)),
            ]
            .into(),
        )];

        mi.index.add(id5, indexer_result5).unwrap();
        mi.index.commit().unwrap();

        let retrieved_ids = search(&mi, query![eq!["year", 2000]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4, id5]);

        let retrieved_ids = search(
            &mi,
            query![and![
                eq!["year", 2000],
                not![and![
                    eq!["year", 2000],
                    eq!["name", "Empire State Building"]
                ]]
            ]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(
            &mi,
            query![and![
                eq!["year", 2000],
                not![and![eq!["year", 2000], eq!["name", "Main Tower"]]]
            ]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![id5]);

        let retrieved_ids = search(&mi, query![or![eq!["year", 1931], not![or![]]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        let retrieved_ids = search(&mi, query![not![or![]]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id1, mi.id2, mi.id3, mi.id4, id5]);
    }

    #[test]
    fn bbox() {
        let mi = make_mini_index();
        let retrieved_ids = search(&mi, query![bbox![0.0, -90.0, 180.0, 90.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id4]);

        let retrieved_ids = search(&mi, query![bbox![-180.0, -90.0, 0.0, 90.0]]);
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        // exact bbox of id2
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.986479, 40.747914, -73.984866, 40.748978]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        // move query bbox left
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.986489, 40.747914, -73.986479, 40.748978]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        // move query bbox even further left
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.986489, 40.747914, -73.986480, 40.748978]],
        );
        assert_that!(retrieved_ids).is_empty();

        // move query bbox right
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.984866, 40.747914, -73.984856, 40.748978]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        // move query bbox even further right
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.984865, 40.747914, -73.984856, 40.748978]],
        );
        assert_that!(retrieved_ids).is_empty();

        // move query bbox up
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.986479, 40.747904, -73.984866, 40.747914]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        // move query bbox even further up
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.986479, 40.747904, -73.984866, 40.747913]],
        );
        assert_that!(retrieved_ids).is_empty();

        // move query bbox down
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.986479, 40.748978, -73.984866, 40.748988]],
        );
        assert_that!(retrieved_ids).contains_exactly(vec![mi.id2]);

        // move query bbox even further down
        let retrieved_ids = search(
            &mi,
            query![bbox![-73.986479, 40.748979, -73.984866, 40.748988]],
        );
        assert_that!(retrieved_ids).is_empty()
    }

    /// Tests if we can search over a large number of elements - basically
    /// tests [`super::DocSetIterator`]
    #[test]
    fn scrolling() {
        let li = make_large_index(0);
        assert_that!(li
            .index
            .index
            .reader()
            .unwrap()
            .searcher()
            .segment_readers()
            .len())
        .is_equal_to(0);
        assert_that!(search_large(&li, query![])).is_empty();

        let li = make_large_index(20);
        assert_that!(li
            .index
            .index
            .reader()
            .unwrap()
            .searcher()
            .segment_readers()
            .len())
        .is_equal_to(1);
        assert_that!(search_large(&li, query![])).has_length(20);

        let li = make_large_index(COLLECT_BLOCK_BUFFER_LEN / 2);
        assert_that!(search_large(&li, query![])).has_length(COLLECT_BLOCK_BUFFER_LEN / 2);

        let li = make_large_index(COLLECT_BLOCK_BUFFER_LEN);
        assert_that!(search_large(&li, query![])).has_length(COLLECT_BLOCK_BUFFER_LEN);

        let li = make_large_index(COLLECT_BLOCK_BUFFER_LEN * 2);
        assert_that!(search_large(&li, query![])).has_length(COLLECT_BLOCK_BUFFER_LEN * 2);
    }
}
