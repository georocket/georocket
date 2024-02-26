/// Specifies a bounding box.
struct BoundingBox([f64; 4]);

/// Specifies primitive values which may be part of a key:value search in
/// the properties of the features.
enum Value {
    Integer(u64),
    Float(f64),
    String(String),
}

/// Specifies primitive which may be queried for directly.
pub enum Primitive {
    String(String),
    BoundingBox(BoundingBox),
}

/// Specifies the logical combinator to be to combine the list of `QueryComponent`s.
pub enum Logic {
    Or(Vec<QueryComponent>),
    And(Vec<QueryComponent>),
    Not(Box<QueryComponent>),
}

/// Specifies the comparison to be applied in the `QueryComponent::Comparison` variant.
pub enum Comparison {
    Eq,
    Lt,
    Gt,
    Lte,
    Gte,
}

/// The top level components of a `Query` which are
pub enum QueryComponent {
    Primitive(Primitive),
    Logical(Logic),
    Comparison {
        operator: Comparison,
        key: String,
        value: Value,
    },
}

pub struct Query {
    pub components: Vec<QueryComponent>,
}
