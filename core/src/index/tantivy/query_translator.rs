use anyhow::{bail, Result};
use geo::Rect;
use std::ops::Bound;

use tantivy::{
    query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query as TantivyQuery, TermQuery},
    schema::{Field, IndexRecordOption},
    Term,
};

use crate::{
    index::{h3_term_index::TermOptions, Value},
    query::{Logical, Operator, Query, QueryPart},
};

use super::{
    bounding_box_query::{BoundingBoxFields, BoundingBoxQuery},
    json_range_query::JsonRangeQuery,
    Fields,
};

pub(super) struct QueryTranslator<'a> {
    index: &'a tantivy::Index,
    fields: &'a Fields,
    bbox_term_options: TermOptions,
}

impl<'a> QueryTranslator<'a> {
    pub fn new(
        index: &'a tantivy::Index,
        fields: &'a Fields,
        bbox_term_options: TermOptions,
    ) -> Self {
        Self {
            index,
            fields,
            bbox_term_options,
        }
    }

    pub fn translate(&self, query: Query) -> Result<Box<dyn TantivyQuery>> {
        Ok(match query {
            Query::Empty => Box::new(AllQuery),
            Query::Full(p) => self
                .translate_query_part(p)?
                .unwrap_or_else(|| Box::new(AllQuery)),
        })
    }

    fn translate_query_part(&self, part: QueryPart) -> Result<Option<Box<dyn TantivyQuery>>> {
        Ok(match part {
            QueryPart::Value(p) => self.translate_value(p)?,
            QueryPart::BoundingBox(b) => Some(self.translate_bbox(b)?),
            QueryPart::Logical(l) => self.translate_logical(l)?,
            QueryPart::Comparison {
                operator,
                key,
                value,
            } => Some(self.translate_comparison(operator, &key, &value)?),
        })
    }

    fn translate_query_parts(&self, parts: Vec<QueryPart>) -> Result<Vec<Box<dyn TantivyQuery>>> {
        let mut result = Vec::new();
        for p in parts {
            if let Some(r) = self.translate_query_part(p)? {
                result.push(r);
            }
        }
        Ok(result)
    }

    /// Create a query that performs a full-text search on the 'all_values' field
    fn translate_value(&self, value: Value) -> Result<Option<Box<dyn TantivyQuery>>> {
        let text = match value {
            Value::String(s) => s,
            Value::Float(f) => f.to_string(),
            Value::Integer(i) => i.to_string(),
        };

        // tokenize value and create individual terms
        let mut terms = Vec::new();
        let mut tokenizer = self.index.tokenizer_for_field(self.fields.all_values)?;
        let mut token_stream = tokenizer.token_stream(&text);
        token_stream.process(&mut |token| {
            terms.push(Term::from_field_text(self.fields.all_values, &token.text));
        });

        // combine terms
        Ok(if terms.is_empty() {
            None
        } else if terms.len() == 1 {
            let term = terms.into_iter().next().unwrap();
            Some(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
        } else {
            Some(Box::new(PhraseQuery::new(terms)))
        })
    }

    /// Create a spatial query returning chunks whose bounding boxes intersect
    /// the given one
    fn translate_bbox(&self, bbox: Rect) -> Result<Box<dyn TantivyQuery>> {
        Ok(Box::new(BoundingBoxQuery::new(
            self.fields.bbox_terms,
            bbox,
            BoundingBoxFields {
                // TODO is it possible to use self.fields instead of field names (strings)?
                min_x: "bbox_min_x".to_string(),
                min_y: "bbox_min_y".to_string(),
                max_x: "bbox_max_x".to_string(),
                max_y: "bbox_max_y".to_string(),
            },
            self.bbox_term_options,
        )?))
    }

    fn key_value_to_bound(field: Field, key: &str, value: &Value) -> Result<Vec<u8>> {
        let mut bound = Term::from_field_json_path(field, key, false);
        match value {
            Value::String(_) => bail!("Range queries on strings are not supported"),
            Value::Float(f) => bound.append_type_and_fast_value(*f),
            Value::Integer(i) => bound.append_type_and_fast_value(*i),
        };
        Ok(bound.serialized_value_bytes().to_owned())
    }

    /// Returns a new value representing the maximum of the given one
    fn value_max(value: &Value) -> Result<Value> {
        match value {
            Value::String(_) => bail!("Cannot get maximum of string value"),
            Value::Float(_) => Ok(Value::Float(f64::MAX)),
            Value::Integer(_) => Ok(Value::Integer(i64::MAX)),
        }
    }

    /// Returns a new value representing the minimum of the given one
    fn value_min(value: &Value) -> Result<Value> {
        match value {
            Value::String(_) => bail!("Cannot get minimum of string value"),
            Value::Float(_) => Ok(Value::Float(f64::MIN)),
            Value::Integer(_) => Ok(Value::Integer(i64::MIN)),
        }
    }

    fn translate_comparison(
        &self,
        operator: Operator,
        key: &str,
        value: &Value,
    ) -> Result<Box<dyn TantivyQuery>> {
        match operator {
            Operator::Eq => {
                let mut term = Term::from_field_json_path(self.fields.gen_attrs, key, false);
                match value {
                    Value::String(s) => term.append_type_and_str(s),
                    Value::Float(f) => term.append_type_and_fast_value(*f),
                    Value::Integer(i) => term.append_type_and_fast_value(*i),
                };
                Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
            }

            Operator::Gt => {
                let lower_bound = Self::key_value_to_bound(self.fields.gen_attrs, key, value)?;
                let upper_bound =
                    Self::key_value_to_bound(self.fields.gen_attrs, key, &Self::value_max(value)?)?;

                let rq = JsonRangeQuery::new(
                    self.fields.gen_attrs,
                    Bound::Excluded(lower_bound),
                    Bound::Included(upper_bound),
                );

                Ok(Box::new(rq))
            }

            Operator::Gte => {
                let lower_bound = Self::key_value_to_bound(self.fields.gen_attrs, key, value)?;
                let upper_bound =
                    Self::key_value_to_bound(self.fields.gen_attrs, key, &Self::value_max(value)?)?;

                let rq = JsonRangeQuery::new(
                    self.fields.gen_attrs,
                    Bound::Included(lower_bound),
                    Bound::Included(upper_bound),
                );

                Ok(Box::new(rq))
            }

            Operator::Lt => {
                let lower_bound =
                    Self::key_value_to_bound(self.fields.gen_attrs, key, &Self::value_min(value)?)?;
                let upper_bound = Self::key_value_to_bound(self.fields.gen_attrs, key, value)?;

                let rq = JsonRangeQuery::new(
                    self.fields.gen_attrs,
                    Bound::Included(lower_bound),
                    Bound::Excluded(upper_bound),
                );

                Ok(Box::new(rq))
            }

            Operator::Lte => {
                let lower_bound =
                    Self::key_value_to_bound(self.fields.gen_attrs, key, &Self::value_min(value)?)?;
                let upper_bound = Self::key_value_to_bound(self.fields.gen_attrs, key, value)?;

                let rq = JsonRangeQuery::new(
                    self.fields.gen_attrs,
                    Bound::Included(lower_bound),
                    Bound::Included(upper_bound),
                );

                Ok(Box::new(rq))
            }
        }
    }

    fn translate_logical(&self, logical: Logical) -> Result<Option<Box<dyn TantivyQuery>>> {
        match logical {
            Logical::Or(parts) => {
                let operands = self.translate_query_parts(parts)?;
                Ok(if operands.len() > 1 {
                    Some(Box::new(BooleanQuery::new(
                        operands.into_iter().map(|q| (Occur::Should, q)).collect(),
                    )))
                } else {
                    operands.into_iter().next()
                })
            }

            Logical::And(parts) => {
                let operands = self.translate_query_parts(parts)?;
                Ok(if operands.len() > 1 {
                    Some(Box::new(BooleanQuery::new(
                        operands.into_iter().map(|q| (Occur::Must, q)).collect(),
                    )))
                } else {
                    operands.into_iter().next()
                })
            }

            Logical::Not(part) => {
                let operands = self.translate_query_part(*part)?;
                Ok(operands.map::<Box<dyn TantivyQuery>, _>(|sub| {
                    let q = vec![
                        (Occur::MustNot, sub),
                        // return all other documents that don't match the query
                        (Occur::Should, Box::new(AllQuery)),
                    ];
                    Box::new(BooleanQuery::new(q))
                }))
            }
        }
    }
}
