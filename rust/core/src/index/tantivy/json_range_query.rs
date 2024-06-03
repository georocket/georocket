use std::io;
use std::ops::Bound;

use tantivy::query::{
    BitSetDocSet, ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight,
};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::termdict::{TermDictionary, TermStreamer};
use tantivy::{DocId, Result, Score, SegmentReader, TantivyError};
use tantivy_common::BitSet;

/// A workaround for the fact that Tantivy does not support range requests on
/// JSON fields yet (see issue https://github.com/quickwit-oss/tantivy/issues/1709).
///
/// Most of this code has been copied from https://github.com/quickwit-oss/tantivy/blob/0.22.0/src/query/range_query/range_query.rs
/// (licensed under the MIT license, Copyright (c) 2018 by the Tantivy project
/// authors) but adapted for JSON fields.
///
/// Once the aforementioned issue has been resolved, we can remove this code here.
///
/// # Example
///
/// ```rust
/// use std::collections::BTreeMap;
/// use std::ops::Bound;
/// use tantivy::collector::Count;
/// use tantivy::schema::{OwnedValue, Schema, STRING};
/// use tantivy::{doc, Index, IndexWriter, TantivyDocument, Term};
/// use core::index::tantivy::json_range_query::JsonRangeQuery;
/// # fn test() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let attrs_field = schema_builder.add_json_field("attrs", STRING);
/// let schema = schema_builder.build();
///
/// let index = Index::create_in_ram(schema);
/// let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
/// for year in 1950i64..2017i64 {
///     let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
///     for _ in 0..num_docs_within_year {
///         let mut attrs = BTreeMap::new();
///         attrs.insert("year".to_string(), OwnedValue::I64(year));
///         let mut doc = TantivyDocument::new();
///         doc.add_object(attrs_field, attrs);
///         index_writer.add_document(doc)?;
///     }
/// }
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let mut lower_bound_term = Term::from_field_json_path(attrs_field, "year", false);
/// lower_bound_term.append_type_and_fast_value(1960i64);
/// let lower_bound = Bound::Included(lower_bound_term.serialized_value_bytes().to_owned());
///
/// let mut upper_bound_term = Term::from_field_json_path(attrs_field, "year", false);
/// upper_bound_term.append_type_and_fast_value(1970i64);
/// let upper_bound = Bound::Excluded(upper_bound_term.serialized_value_bytes().to_owned());
///
/// let docs_in_the_sixties = JsonRangeQuery::new(attrs_field, lower_bound, upper_bound);
/// let num_60s_books = searcher.search(&docs_in_the_sixties, &Count)?;
/// assert_eq!(num_60s_books, 2285);
/// Ok(())
/// # }
/// # assert!(test().is_ok());
/// ```

#[derive(Clone, Debug)]
pub struct JsonRangeQuery {
    field: Field,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
}

impl JsonRangeQuery {
    pub fn new(field: Field, lower_bound: Bound<Vec<u8>>, upper_bound: Bound<Vec<u8>>) -> Self {
        JsonRangeQuery {
            field,
            lower_bound,
            upper_bound,
        }
    }
}

impl Query for JsonRangeQuery {
    fn weight(&self, _: EnableScoring<'_>) -> Result<Box<dyn Weight>> {
        Ok(Box::new(JsonRangeWeight {
            field: self.field,
            lower_bound: self.lower_bound.clone(),
            upper_bound: self.upper_bound.clone(),
        }))
    }
}

pub struct JsonRangeWeight {
    field: Field,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
}

impl JsonRangeWeight {
    fn term_range<'a>(&self, term_dict: &'a TermDictionary) -> io::Result<TermStreamer<'a>> {
        use std::ops::Bound::*;
        let mut term_stream_builder = term_dict.range();
        term_stream_builder = match self.lower_bound {
            Included(ref term_val) => term_stream_builder.ge(term_val),
            Excluded(ref term_val) => term_stream_builder.gt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder = match self.upper_bound {
            Included(ref term_val) => term_stream_builder.le(term_val),
            Excluded(ref term_val) => term_stream_builder.lt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder.into_stream()
    }
}

impl Weight for JsonRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_range = self.term_range(term_dict)?;
        while term_range.advance() {
            let term_info = term_range.value();
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            loop {
                let docs = block_segment_postings.docs();
                if docs.is_empty() {
                    break;
                }
                for &doc in block_segment_postings.docs() {
                    doc_bitset.insert(doc);
                }
                block_segment_postings.advance();
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({}) does not match",
                doc
            )));
        }
        Ok(Explanation::new("JsonRangeQuery", 1.0))
    }
}
