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

/// Specifies the logical combinator to be to combine the list of `QueryComponent`s.
#[derive(Debug, PartialEq)]
pub enum Logic {
    Or(Vec<QueryComponent>),
    And(Vec<QueryComponent>),
    Not(Vec<QueryComponent>),
}

/// Specifies how two key-value pairs should be compared to each other
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Comparison {
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

impl Display for Comparison {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Comparison::Eq => write!(f, "="),
            Comparison::Lt => write!(f, "<"),
            Comparison::Gt => write!(f, ">"),
            Comparison::Lte => write!(f, "<="),
            Comparison::Gte => write!(f, ">="),
        }
    }
}

/// The top level components of a `Query`
#[derive(Debug, PartialEq)]
pub enum QueryComponent {
    Primitive(Primitive),
    Logical(Logic),
    Comparison {
        operator: Comparison,
        key: String,
        value: Value,
    },
}

impl<P> From<P> for QueryComponent
where
    P: Into<Primitive>,
{
    fn from(primitive: P) -> Self {
        QueryComponent::Primitive(primitive.into())
    }
}

impl From<Logic> for QueryComponent {
    fn from(logic: Logic) -> Self {
        QueryComponent::Logical(logic)
    }
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub components: Vec<QueryComponent>,
}

impl From<Vec<QueryComponent>> for Query {
    fn from(components: Vec<QueryComponent>) -> Self {
        Query { components }
    }
}

macro_rules! query {
    ($($x:expr),* $(,)?) => {
        $crate::query::Query { components: vec![$($x.into(),)*] }
    };
}

macro_rules! and {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryComponent::Logical(
            $crate::query::Logic::And(vec![$($x.into(),)*]))
    };
}

macro_rules! or {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryComponent::Logical(
            $crate::query::Logic::Or(vec![$($x.into(),)*]))
    };
}

macro_rules! not {
    ($($x:expr),* $(,)?) => {
        $crate::query::QueryComponent::Logical(
            $crate::query::Logic::Not(vec![$($x.into(),)*]))
    };
}

macro_rules! eq {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryComponent::Comparison {
            operator: $crate::query::Comparison::Eq,
            key,
            value,
        }
    }};
}

macro_rules! gt {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryComponent::Comparison {
            operator: $crate::query::Comparison::Gt,
            key,
            value,
        }
    }};
}

macro_rules! gte {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryComponent::Comparison {
            operator: $crate::query::Comparison::Gte,
            key,
            value,
        }
    }};
}

macro_rules! lt {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryComponent::Comparison {
            operator: $crate::query::Comparison::Lt,
            key,
            value,
        }
    }};
}

macro_rules! lte {
    ($key:expr, $value:expr) => {{
        let key = $key.into();
        let value = $value.into();
        $crate::query::QueryComponent::Comparison {
            operator: $crate::query::Comparison::Lte,
            key,
            value,
        }
    }};
}

pub(crate) use and;
pub(crate) use eq;
pub(crate) use gt;
pub(crate) use gte;
pub(crate) use lt;
pub(crate) use lte;
pub(crate) use not;
pub(crate) use or;
pub(crate) use query;
