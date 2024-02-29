use std::fmt::{Display, Formatter};

/// Specifies a bounding box.
#[derive(Debug)]
pub(super) struct BoundingBox(pub(super) [f64; 4]);

impl BoundingBox {
    pub(super) fn to_geo_json(&self) -> String {
        let left = self.0[0];
        let right = self.0[2];
        let bottom = self.0[1];
        let top = self.0[3];
        format!(
            r#"{{"type":"Polygon","coordinates":[[[{}, {}],[{}, {}],[{}, {}],[{}, {}],[{}, {}]]]}}"#,
            left, bottom, right, bottom, right, top, left, top, left, bottom
        )
    }
}

/// Specifies primitive values which may be part of a key:value search in
/// the properties of the features.
#[derive(Debug)]
pub enum Value {
    Integer(u64),
    Float(f64),
    String(String),
}

/// Specifies primitive which may be queried for directly.
#[derive(Debug)]
pub enum Primitive {
    String(String),
    BoundingBox(BoundingBox),
}

/// Specifies the logical combinator to be to combine the list of `QueryComponent`s.
#[derive(Debug)]
pub enum Logic {
    Or(Vec<QueryComponent>),
    And(Vec<QueryComponent>),
    Not(Box<QueryComponent>),
}

/// Specifies the comparison to be applied in the `QueryComponent::Comparison` variant.
#[derive(Copy, Clone, Debug)]
pub enum Comparison {
    Eq,
    Lt,
    Gt,
    Lte,
    Gte,
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

#[derive(Debug)]
pub struct Query {
    pub components: Vec<QueryComponent>,
}
