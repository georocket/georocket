use std::fmt::{Display, Formatter};

use crate::index::Value;

pub mod dsl;

/// Specifies primitive which may be queried for directly.
#[derive(Debug, PartialEq)]
pub enum Primitive {
    String(String),
    Number(f64),
}

impl From<String> for Primitive {
    fn from(value: String) -> Self {
        Primitive::String(value)
    }
}

impl From<&str> for Primitive {
    fn from(value: &str) -> Self {
        Primitive::String(value.into())
    }
}

impl From<f64> for Primitive {
    fn from(value: f64) -> Self {
        Primitive::Number(value)
    }
}

/// Specifies the logical combinator to be to combine the list of `QueryPart`s.
#[derive(Debug, PartialEq)]
pub enum Logic {
    Or(Vec<QueryPart>),
    And(Vec<QueryPart>),
    Not(Vec<QueryPart>),
}

/// Specifies how two key-value pairs should be compared to each other
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Operator {
    /// The values must equal
    Eq,

    /// The value of this key-value pair must be greater than the other one
    Gt,

    /// The value of this key-value pair must be greater than or equal to the
    /// other one
    Gte,

    /// The value of this key-value pair must be less than the other one
    Lt,

    /// The value of this key-value pair must be less than or equal to the
    /// other one
    Lte,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Eq => write!(f, "="),
            Operator::Lt => write!(f, "<"),
            Operator::Gt => write!(f, ">"),
            Operator::Lte => write!(f, "<="),
            Operator::Gte => write!(f, ">="),
        }
    }
}

/// The top level parts of a `Query`
#[derive(Debug, PartialEq)]
pub enum QueryPart {
    Primitive(Primitive),
    Logical(Logic),
    Comparison {
        operator: Operator,
        key: String,
        value: Value,
    },
}

impl<P> From<P> for QueryPart
where
    P: Into<Primitive>,
{
    fn from(primitive: P) -> Self {
        QueryPart::Primitive(primitive.into())
    }
}

impl From<Logic> for QueryPart {
    fn from(logic: Logic) -> Self {
        QueryPart::Logical(logic)
    }
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub parts: Vec<QueryPart>,
}

impl From<Vec<QueryPart>> for Query {
    fn from(parts: Vec<QueryPart>) -> Self {
        Query { parts }
    }
}

#[cfg(test)]
macro_rules! query {
    ($($x:expr),* $(,)?) => {
        $crate::query::Query { parts: vec![$($x.into(),)*] }
    };
}

#[cfg(test)]
macro_rules! and {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryPart::Logical(
            $crate::query::Logic::And(vec![$($x.into(),)*]))
    };
}

#[cfg(test)]
macro_rules! or {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryPart::Logical(
            $crate::query::Logic::Or(vec![$($x.into(),)*]))
    };
}

#[cfg(test)]
macro_rules! not {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryPart::Logical(
            $crate::query::Logic::Not(vec![$($x.into(),)*]))
    };
}

#[cfg(test)]
macro_rules! eq {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Eq,
            key,
            value,
        }
    }};
}

#[cfg(test)]
macro_rules! gt {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Gt,
            key,
            value,
        }
    }};
}

#[cfg(test)]
macro_rules! gte {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Gte,
            key,
            value,
        }
    }};
}

#[cfg(test)]
macro_rules! lt {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Lt,
            key,
            value,
        }
    }};
}

#[cfg(test)]
macro_rules! lte {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryPart::Comparison {
            operator: $crate::query::Operator::Lte,
            key,
            value,
        }
    }};
}

#[cfg(test)]
pub(crate) use and;
#[cfg(test)]
pub(crate) use eq;
#[cfg(test)]
pub(crate) use gt;
#[cfg(test)]
pub(crate) use gte;
#[cfg(test)]
pub(crate) use lt;
#[cfg(test)]
pub(crate) use lte;
#[cfg(test)]
pub(crate) use not;
#[cfg(test)]
pub(crate) use or;
#[cfg(test)]
pub(crate) use query;
