//! The `attributes` module contains a type definition for [`Attributes`] as well as the
//! [`Value`] enum used. It also contains the [`AttributesBuilder`] which can be used to aid in
//! the creation of an `Attributes` instance.
//!
//! # Strings
//!
//! Adding a string to the `AttributesBuilder` will result in an entry with the `Value::String`
//! variant:
//! ```
//! # use indexing::attributes::*;
//! let mut attributes = AttributesBuilder::new()
//!         .add_attribute("key".to_string(), "value".to_string())
//!         .build();
//! assert_eq!(attributes.get("key"), Some(&Value::String("value".to_string())));
//! ```
//!
//! # Doubles and Integers
//!
//! Adding a double or integer to the `AttributesBuilder` will result in an entry with the
//! `Value::Double` or `Value::Integer` variant respectively:
//!
//! ```
//! # use indexing::attributes::*;
//! let attributes = AttributesBuilder::new()
//!     .add_attribute("double".to_string(), "1.624".to_string())
//!     .add_attribute("integer".to_string(), "42".to_string())
//!     .build();
//! assert_eq!(attributes.get("double"), Some(&Value::Double(1.624)));
//! assert_eq!(attributes.get("integer"), Some(&Value::Integer(42)));
//! ```
//!
//! # Malformed numbers
//!
//! Attempting to add a malformed number will simply result in an entry with the value as the
//! `Value::String` variant:
//!
//! ```
//! # use indexing::attributes::*;
//! let attributes = AttributesBuilder::new()
//!     .add_attribute("malformed_double".to_string(), "1.624.42".to_string())
//!     .add_attribute("malformed_integer".to_string(), "4E".to_string())
//!     .build();
//! assert_eq!(attributes.get("malformed_double"), Some(&Value::String("1.624.42".to_string())));
//! assert_eq!(attributes.get("malformed_integer"), Some(&Value::String("4E".to_string())));
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A type representing the different values an attribute can have.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Double(f64),
    Integer(i64),
}

/// A type representing a collection of attributes as key-value pairs stored in a hashmap.
/// The key is always a `String` and the value is of the [`Value`] type and can thus be either a
/// String itself or a `f64` or `i64`.
pub type Attributes = HashMap<String, Value>;

/// A builder to create [`Attributes`].
///
/// The builder takes key-value pairs representing the attributes to construct an `Attributes`
pub struct AttributesBuilder {
    attributes: Attributes,
}

impl AttributesBuilder {
    /// Creates a new `AttributesBuilder`.
    pub fn new() -> Self {
        Self {
            attributes: HashMap::new(),
        }
    }

    /// Adds a key-value pair to the builder. If the value can be parsed into an integer or double,
    /// it will be stored as a `Value::Integer` or `Value::Double` respectively. Otherwise, it will
    /// be stored as a `Value::String`.
    #[must_use]
    pub fn add_attribute(mut self, key: impl Into<String>, value: &str) -> Self {
        self.add_attribute_mut(key, value);
        self
    }

    pub fn add_attribute_mut(&mut self, key: impl Into<String>, value: &str) -> &mut Self {
        let key = key.into();
        let value: Value = {
            if let Ok(integer) = value.parse::<i64>() {
                Value::Integer(integer)
            } else if let Ok(double) = value.parse::<f64>() {
                Value::Double(double)
            } else {
                Value::String(value.into())
            }
        };
        self.attributes.entry(key).or_insert(value);
        self
    }
    /// Builds the `Attributes` from the key-value pairs added to the builder.
    pub fn build(self) -> Attributes {
        self.attributes
    }
}

impl Default for AttributesBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_attribute() {
        let attributes = AttributesBuilder::new().build();
        assert!(attributes.is_empty());
    }

    #[test]
    fn basic_values() {
        let mut attributes = AttributesBuilder::new()
            .add_attribute("string".to_string(), "elephant")
            .add_attribute("double".to_string(), "1.624")
            .add_attribute("integer".to_string(), "42");
        attributes.add_attribute_mut("another_int".to_string(), "23");
        let attributes = attributes.build();
        assert_eq!(
            attributes.get("string"),
            Some(&Value::String("elephant".to_string()))
        );
        assert_eq!(attributes.get("double"), Some(&Value::Double(1.624)));
        assert_eq!(attributes.get("integer"), Some(&Value::Integer(42)));
    }
}
