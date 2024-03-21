use georocket_types::{BoundingBox, Value};
use std::fmt::{Display, Formatter};

/// Specifies primitive which may be queried for directly.
#[derive(Debug)]
pub enum Primitive {
    String(String),
    BoundingBox(BoundingBox),
}

impl From<BoundingBox> for Primitive {
    fn from(bounding_box: BoundingBox) -> Self {
        Primitive::BoundingBox(bounding_box)
    }
}

impl From<String> for Primitive {
    fn from(value: String) -> Self {
        Primitive::String(value.into())
    }
}

impl From<&str> for Primitive {
    fn from(value: &str) -> Self {
        Primitive::String(value.into())
    }
}

/// Specifies the logical combinator to be to combine the list of `QueryComponent`s.
#[derive(Debug)]
pub enum Logic {
    Or(Vec<QueryComponent>),
    And(Vec<QueryComponent>),
    Not(Box<QueryComponent>),
}

impl Logic {
    pub fn not(qc: impl Into<QueryComponent>) -> Self {
        Self::Not(Box::new(qc.into()))
    }
}

/// Specifies the comparison to be applied in the `QueryComponent::Comparison` variant.
#[derive(Copy, Clone, Debug)]
pub enum Comparison {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
}

impl Display for Comparison {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Comparison::Eq => write!(f, "="),
            Comparison::Neq => write!(f, "!="),
            Comparison::Lt => write!(f, "<"),
            Comparison::Gt => write!(f, ">"),
            Comparison::Lte => write!(f, "<="),
            Comparison::Gte => write!(f, ">="),
        }
    }
}

/// The top level components of a `Query` which are
#[derive(Debug)]
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

impl<K, V> From<(Comparison, K, V)> for QueryComponent
where
    K: Into<String>,
    V: Into<Value>,
{
    fn from((operator, key, value): (Comparison, K, V)) -> Self {
        let key = key.into();
        let value = value.into();
        QueryComponent::Comparison {
            operator,
            key,
            value,
        }
    }
}

#[derive(Debug)]
pub struct Query {
    pub components: Vec<QueryComponent>,
}
