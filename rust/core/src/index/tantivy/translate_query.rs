use anyhow::Result;
use std::borrow::Cow;

use tantivy::{
    query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query as TantivyQuery, TermQuery},
    schema::IndexRecordOption,
    Term,
};

use crate::{
    index::Value,
    query::{Operator, Query, QueryPart},
};

use super::Fields;

pub(super) struct QueryTranslator<'a> {
    index: &'a tantivy::Index,
    fields: &'a Fields,
}

impl<'a> QueryTranslator<'a> {
    pub fn new(index: &'a tantivy::Index, fields: &'a Fields) -> Self {
        Self { index, fields }
    }

    pub fn translate(&self, query: Query) -> Result<Box<dyn TantivyQuery>> {
        Ok(self
            .translate_query_parts(query.parts)?
            .unwrap_or_else(|| Box::new(AllQuery)))
    }

    fn translate_query_parts(
        &self,
        parts: Vec<QueryPart>,
    ) -> Result<Option<Box<dyn TantivyQuery>>> {
        let mut operands = Vec::new();
        for p in parts {
            let q = match p {
                QueryPart::Value(p) => self.translate_value(&p)?,
                QueryPart::Logical(_) => todo!(),
                QueryPart::Comparison {
                    operator,
                    key,
                    value,
                } => Some(self.translate_comparison(operator, &key, &value)),
            };
            if let Some(q) = q {
                operands.push(q);
            }
        }

        Ok(if operands.len() > 1 {
            Some(Box::new(BooleanQuery::new(
                operands.into_iter().map(|q| (Occur::Should, q)).collect(),
            )))
        } else {
            operands.into_iter().next()
        })
    }

    /// Create a query that performs a full-text search on the 'all_values' field
    fn translate_value(&self, value: &Value) -> Result<Option<Box<dyn TantivyQuery>>> {
        let text = match value {
            Value::String(s) => Cow::Borrowed(s),
            Value::Float(f) => Cow::Owned(f.to_string()),
            Value::Integer(i) => Cow::Owned(i.to_string()),
        };

        // tokenize value and create individual terms
        let mut terms = Vec::new();
        let mut tokenizer = self
            .index
            .tokenizer_for_field(self.fields.all_values_field)?;
        let mut token_stream = tokenizer.token_stream(&text);
        token_stream.process(&mut |token| {
            terms.push(Term::from_field_text(
                self.fields.all_values_field,
                &token.text,
            ));
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

    fn translate_comparison(
        &self,
        operator: Operator,
        key: &str,
        value: &Value,
    ) -> Box<dyn TantivyQuery> {
        Box::new(match operator {
            Operator::Eq => {
                let mut term = Term::from_field_json_path(self.fields.gen_attrs_field, key, false);
                match value {
                    Value::String(s) => term.append_type_and_str(s),
                    Value::Float(f) => term.append_type_and_fast_value(*f),
                    Value::Integer(i) => term.append_type_and_fast_value(*i),
                };
                TermQuery::new(term, IndexRecordOption::Basic)
            }

            Operator::Gt => todo!(),
            Operator::Gte => todo!(),
            Operator::Lt => todo!(),
            Operator::Lte => todo!(),
        })
    }
}
