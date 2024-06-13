use std::sync::Arc;

use anyhow::Result;
use geo::{coord, Rect};
use tantivy::{
    columnar::Column,
    query::{EnableScoring, Explanation, Query, Scorer, TermSetQuery, Weight},
    schema::Field,
    DocId, DocSet, Score, SegmentReader, TantivyError, Term, TERMINATED,
};

use crate::{
    index::h3_term_index::{make_terms, TermMode, TermOptions},
    util::bounding_box::BoundingBox,
};

/// A query that filters documents based on a bounding box. The query first
/// tries to find candidates using index terms generated by
/// [`crate::index::h3_term_index::make_terms`]. It then checks `f64`
/// [`tantivy::schema::FAST`] `fields` to get the bounding box of each candidate
/// and returns only those candidates that actually intersect with the given
/// bounding box.
#[derive(Clone, Debug)]
pub struct BoundingBoxQuery {
    bbox: BoundingBox,
    query: TermSetQuery,
    fields: Arc<BoundingBoxFields>,
}

/// The name of the fields that contain the bounding box. The fields must be
/// [`tantivy::schema::FAST`] fields and their type must be `f64`.
#[derive(Clone, Debug)]
pub struct BoundingBoxFields {
    pub min_x: String,
    pub min_y: String,
    pub max_x: String,
    pub max_y: String,
}

impl BoundingBoxQuery {
    /// Create a new bounding box query for a field that stores index terms
    /// generated by [`crate::index::h3_term_index::make_terms`]. `fields`
    /// refers to f64 [`tantivy::schema::FAST`] fields containing the actual
    /// bounding box, so candidates can be filtered out correctly.
    pub fn new(
        field: Field,
        bbox: BoundingBox,
        fields: BoundingBoxFields,
        term_options: TermOptions,
    ) -> Result<Self> {
        let rect = Rect::new(
            coord! { x: bbox.min_x, y: bbox.min_y },
            coord! { x: bbox.max_x, y: bbox.max_y },
        );
        let terms = make_terms(&rect, term_options, TermMode::Query)?;
        let query = TermSetQuery::new(terms.into_iter().map(|s| Term::from_field_text(field, &s)));

        Ok(Self {
            bbox,
            query,
            fields: Arc::new(fields),
        })
    }
}

impl Query for BoundingBoxQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(BoundingBoxWeight::new(
            self.bbox,
            self.query.weight(enable_scoring)?,
            Arc::clone(&self.fields),
        )))
    }
}

struct BoundingBoxWeight {
    bbox: BoundingBox,
    weight: Box<dyn Weight>,
    fields: Arc<BoundingBoxFields>,
}

impl BoundingBoxWeight {
    fn new(bbox: BoundingBox, weight: Box<dyn Weight>, fields: Arc<BoundingBoxFields>) -> Self {
        Self {
            bbox,
            weight,
            fields,
        }
    }
}

impl Weight for BoundingBoxWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let min_x = reader.fast_fields().f64(&self.fields.min_x)?;
        let min_y = reader.fast_fields().f64(&self.fields.min_y)?;
        let max_x = reader.fast_fields().f64(&self.fields.max_x)?;
        let max_y = reader.fast_fields().f64(&self.fields.max_y)?;
        Ok(Box::new(BoundingBoxScorer::new(
            self.bbox,
            self.weight.scorer(reader, boost)?,
            min_x,
            min_y,
            max_x,
            max_y,
        )))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let mut explanation = Explanation::new("BoundingBox", 1.0);
        let underlying_explanation = self.weight.explain(reader, doc)?;
        explanation.add_detail(underlying_explanation);
        Ok(explanation)
    }
}

/// Wraps around another scorer that returns candidates. Iterates through all
/// candidates and returns only those that actually intersect with the query
/// bounding box.
struct BoundingBoxScorer {
    bbox: BoundingBox,
    scorer: Box<dyn Scorer>,
    min_x_field: Column<f64>,
    min_y_field: Column<f64>,
    max_x_field: Column<f64>,
    max_y_field: Column<f64>,
}

impl BoundingBoxScorer {
    fn new(
        bbox: BoundingBox,
        scorer: Box<dyn Scorer>,
        min_x_field: Column<f64>,
        min_y_field: Column<f64>,
        max_x_field: Column<f64>,
        max_y_field: Column<f64>,
    ) -> Self {
        let mut r = Self {
            bbox,
            scorer,
            min_x_field,
            min_y_field,
            max_x_field,
            max_y_field,
        };

        r.advance_to_first();

        r
    }

    /// Advances the cursor to the first document that intersects with the
    /// bounding box
    fn advance_to_first(&mut self) {
        while self.scorer.doc() != TERMINATED {
            let target = self.scorer.doc();
            if self.intersects(target) {
                break;
            }
            self.scorer.advance();
        }
    }

    /// Checks if a document intersects with the query bounding box
    fn intersects(&self, doc: DocId) -> bool {
        let min_x = self
            .min_x_field
            .values_for_doc(doc)
            .next()
            .unwrap_or(f64::MAX);
        if min_x > self.bbox.max_x {
            return false;
        }

        let min_y = self
            .min_y_field
            .values_for_doc(doc)
            .next()
            .unwrap_or(f64::MAX);
        if min_y > self.bbox.max_y {
            return false;
        }

        let max_x = self
            .max_x_field
            .values_for_doc(doc)
            .next()
            .unwrap_or(f64::MIN);
        if max_x < self.bbox.min_x {
            return false;
        }

        let max_y = self
            .max_y_field
            .values_for_doc(doc)
            .next()
            .unwrap_or(f64::MIN);
        if max_y < self.bbox.min_y {
            return false;
        }

        true
    }
}

impl DocSet for BoundingBoxScorer {
    fn advance(&mut self) -> DocId {
        // advance the cursor to the next document that intersects with the
        // bounding box
        loop {
            let candidate = self.scorer.advance();
            if candidate == TERMINATED {
                return TERMINATED;
            }
            if self.intersects(candidate) {
                return candidate;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        // perform seek on the underlying scorer and then advance to the next
        // document that intersects with the bounding box
        let candidate = self.scorer.seek(target);
        if candidate == TERMINATED {
            return TERMINATED;
        }
        if self.intersects(candidate) {
            return candidate;
        }
        self.advance()
    }

    fn doc(&self) -> DocId {
        self.scorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.scorer.size_hint()
    }
}

impl Scorer for BoundingBoxScorer {
    fn score(&mut self) -> Score {
        self.scorer.score()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assertor::{assert_that, EqualityAssertion};
    use tantivy::{
        columnar::{column_values::VecColumn, Column, ColumnIndex},
        DocSet, TERMINATED,
    };

    use crate::{index::tantivy::iter_scorer::IterScorer, util::bounding_box::BoundingBox};

    use super::BoundingBoxScorer;

    #[test]
    pub fn empty() {
        let bbox = BoundingBox::new(-73.986479, 40.747914, 0.0, -73.984866, 40.748978, 0.0);
        let min_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![])),
        };
        let min_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![])),
        };
        let max_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![])),
        };
        let max_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![])),
        };

        let docs = vec![];
        let mut scorer = BoundingBoxScorer::new(
            bbox,
            Box::new(IterScorer::new(docs.into_iter())),
            min_x_field,
            min_y_field,
            max_x_field,
            max_y_field,
        );

        assert_that!(scorer.doc()).is_equal_to(TERMINATED);
        assert_that!(scorer.advance()).is_equal_to(TERMINATED);
    }

    #[test]
    pub fn no_match() {
        let bbox = BoundingBox::new(-73.986479, 40.747914, 0.0, -73.984866, 40.748978, 0.0);
        let min_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 0.0])),
        };
        let min_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 0.0])),
        };
        let max_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 0.0])),
        };
        let max_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 0.0])),
        };

        let docs = vec![0, 1];
        let mut scorer = BoundingBoxScorer::new(
            bbox,
            Box::new(IterScorer::new(docs.into_iter())),
            min_x_field,
            min_y_field,
            max_x_field,
            max_y_field,
        );

        assert_that!(scorer.doc()).is_equal_to(TERMINATED);
        assert_that!(scorer.advance()).is_equal_to(TERMINATED);
    }

    #[test]
    pub fn two_matches() {
        let bbox = BoundingBox::new(-73.986479, 40.747914, 0.0, -73.984866, 40.748978, 0.0);
        let min_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, -73.985479, 0.0, -73.984866])),
        };
        let min_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 40.748514, 0.0, 40.747914])),
        };
        let max_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, -73.985478, 0.0, -73.984865])),
        };
        let max_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 40.748515, 0.0, 40.748978])),
        };

        let docs = vec![0, 1, 2, 3];
        let mut scorer = BoundingBoxScorer::new(
            bbox,
            Box::new(IterScorer::new(docs.into_iter())),
            min_x_field,
            min_y_field,
            max_x_field,
            max_y_field,
        );

        assert_that!(scorer.doc()).is_equal_to(1);
        assert_that!(scorer.advance()).is_equal_to(3);
        assert_that!(scorer.advance()).is_equal_to(TERMINATED);
    }

    #[test]
    pub fn underlying_skip() {
        let bbox = BoundingBox::new(-73.986479, 40.747914, 0.0, -73.984866, 40.748978, 0.0);
        let min_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, -73.985479, 0.0, -73.984866])),
        };
        let min_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 40.748514, 0.0, 40.747914])),
        };
        let max_x_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, -73.985478, 0.0, -73.984865])),
        };
        let max_y_field = Column {
            index: ColumnIndex::Full,
            values: Arc::new(VecColumn::from(vec![0.0, 40.748515, 0.0, 40.748978])),
        };

        let docs = vec![0, 1, 2];
        let mut scorer = BoundingBoxScorer::new(
            bbox,
            Box::new(IterScorer::new(docs.into_iter())),
            min_x_field,
            min_y_field,
            max_x_field,
            max_y_field,
        );

        assert_that!(scorer.doc()).is_equal_to(1);
        assert_that!(scorer.advance()).is_equal_to(TERMINATED);
    }
}