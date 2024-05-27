use std::collections::HashMap;

/// Represents different types allowed as indexed values
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    String(String),
    Float(f64),
    Integer(i64),
    Object(HashMap<String, Value>),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.into())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
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
