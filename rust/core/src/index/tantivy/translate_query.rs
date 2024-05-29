use tantivy::{
    query::{AllQuery, BooleanQuery, Occur, Query as TantivyQuery, TermQuery},
    schema::IndexRecordOption,
    Term,
};

use crate::{
    index::Value,
    query::{Operator, Query, QueryPart},
};

use super::Fields;

pub fn translate_query(query: Query, fields: &Fields) -> Box<dyn TantivyQuery> {
    translate_query_parts(query.parts, fields).unwrap_or_else(|| Box::new(AllQuery))
}

fn translate_query_parts(parts: Vec<QueryPart>, fields: &Fields) -> Option<Box<dyn TantivyQuery>> {
    let mut operands = Vec::new();
    for p in parts {
        operands.push(match p {
            QueryPart::Primitive(_) => todo!(),
            QueryPart::Logical(_) => todo!(),
            QueryPart::Comparison {
                operator,
                key,
                value,
            } => translate_comparison(operator, &key, &value, fields),
        });
    }

    if operands.len() > 1 {
        Some(Box::new(BooleanQuery::new(
            operands.into_iter().map(|q| (Occur::Should, q)).collect(),
        )))
    } else {
        operands.into_iter().next()
    }
}

fn translate_comparison(
    operator: Operator,
    key: &str,
    value: &Value,
    fields: &Fields,
) -> Box<dyn TantivyQuery> {
    Box::new(match operator {
        Operator::Eq => {
            let mut term = Term::from_field_json_path(fields.gen_attrs_field, key, false);
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
