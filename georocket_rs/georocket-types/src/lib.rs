use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    pub x: f64,
    pub y: f64,
}

impl GeoPoint {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
pub struct BoundingBox {
    pub srid: Option<u32>,
    pub lower_left: GeoPoint,
    pub upper_right: GeoPoint,
}

/// Type representing the different types allowed as values in key-value pairs.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Value {
    String(String),
    Float(f64),
    Integer(i64),
}

impl Value {
    /// Returns a reference to the contained String, panics if the `Value` is not a `Value::String`
    /// variant.
    pub fn unwrap_str(&self) -> &str {
        match self {
            Value::String(s) => s.as_str(),
            _ => panic!("Value does not contain String"),
        }
    }
    /// Returns a copy of the contained float. In case of a `Value::Integer` variant the contained
    /// integer is cast to a float. In case of a `Value::String` variant, attempts to parse the
    /// contained string to a float.
    /// Panics in case of a `Value::String` variant which cannot be parsed to a valid f64.
    pub fn unwrap_float(&self) -> f64 {
        match self {
            Value::String(s) => s.parse().expect("value did not contain a valid float"),
            Value::Float(f) => *f,
            Value::Integer(i) => *i as f64,
        }
    }
    /// Returns a copy of the contained integer. In case of a `Value::Float` variant the contained
    /// integer is cast to an integer. In case of a `Value::String` variant, attempts to parse the
    /// contained string to an integer.
    /// Panics in case of a `Value::String` variant which cannot be parsed to a valid i64.
    pub fn unwrap_int(&self) -> i64 {
        match self {
            Value::String(s) => s.parse().expect("value did not contain a valid int"),
            Value::Float(f) => *f as i64,
            Value::Integer(i) => *i,
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.into())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}
