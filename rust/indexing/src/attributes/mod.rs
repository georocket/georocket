//! The `attributes` module contains a type definition for [`Attributes`] as well as the
//! [`Value`] enum used. It also contains the [`AttributesBuilder`] which can be used to aid in
//! the creation of an `Attributes` instance.
//!
//! # Strings
//!
//! Adding a string to the `AttributesBuilder` will result in an entry with the `Value::String`
//! variant:
//! ```
//! # use georocket_types::Value;
//! # use indexing::attributes::*;
//! let mut attributes = AttributesBuilder::new()
//!         .add_attribute("key".to_string(), "value")
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
//! # use georocket_types::Value;
//! # use indexing::attributes::*;
//! let attributes = AttributesBuilder::new()
//!     .add_attribute("double".to_string(), 1.624)
//!     .add_attribute("integer".to_string(), 42)
//!     .build();
//! assert_eq!(attributes.get("double"), Some(&Value::Float(1.624)));
//! assert_eq!(attributes.get("integer"), Some(&Value::Integer(42)));
//! ```

use georocket_types::Value;
use std::collections::HashMap;

/// A type representing a collection of attributes as key-value pairs stored in a hashmap.
/// The key is always a `String` and the value is of the [`Value`] type.
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
    pub fn add_attribute(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.add_attribute_mut(key, value);
        self
    }

    pub fn add_attribute_mut(
        &mut self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> &mut Self {
        let key = key.into();
        let value = value.into();
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
            .add_attribute("double".to_string(), 1.624)
            .add_attribute("integer".to_string(), 42);
        attributes.add_attribute_mut("another_int".to_string(), 23);
        let attributes = attributes.build();
        assert_eq!(
            attributes.get("string"),
            Some(&Value::String("elephant".to_string()))
        );
        assert_eq!(attributes.get("double"), Some(&Value::Float(1.624)));
        assert_eq!(attributes.get("integer"), Some(&Value::Integer(42)));
    }
}
