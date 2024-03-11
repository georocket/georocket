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
    /// returns the coordinates of the `GeoPoint` as a tuple.
    /// The values of the tuple correspond to: (x, y)
    pub fn as_tuple(&self) -> (f64, f64) {
        (self.x, self.y)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
pub struct BoundingBox {
    pub srid: Option<u32>,
    pub lower_left: GeoPoint,
    pub upper_right: GeoPoint,
}

impl BoundingBox {
    /// Constructs a `BoundingBox` from two `GeoPoints and an optional srid
    ///
    /// # Overhead
    /// `a` and `b` do not represent minimum or maximum points. The function will
    /// extract the minimum and maximum values via comparison.
    /// Construct the `BoundingBox` directly or use [`from_tuple`], if you wish to avoid
    /// this overhead.
    ///
    /// ```
    /// use georocket_types::{BoundingBox, GeoPoint};
    /// let a = GeoPoint::new(42., 1.414);
    /// let b = GeoPoint::new(16.07, 3.141);
    /// let b_box = BoundingBox::new(a, b, None);
    /// assert_eq!(
    ///     b_box,
    ///     BoundingBox{
    ///         srid: None,
    ///         lower_left: GeoPoint::new(16.07, 1.414),
    ///         upper_right: GeoPoint::new(42., 3.141),
    ///     });
    /// ```
    pub fn new(a: GeoPoint, b: GeoPoint, srid: Option<u32>) -> Self {
        let (min_x, max_x) = if a.x < b.x { (a.x, b.x) } else { (b.x, a.x) };
        let (min_y, may_y) = if a.y < b.y { (a.y, b.y) } else { (b.y, a.y) };
        Self {
            srid,
            lower_left: GeoPoint::new(min_x, min_y),
            upper_right: GeoPoint::new(max_x, may_y),
        }
    }
    /// Constructs a `BoundingBox` from a tuple in the form of `(min_x, min_y, max_x, max_y, srid)`.
    /// Asserts that `min_x <= max_x` and `min_y <= max_y` in debug builds.
    pub fn from_tuple(
        (min_x, min_y, max_x, max_y, srid): (f64, f64, f64, f64, Option<u32>),
    ) -> Self {
        debug_assert!(min_x <= max_x, "min_x was larger than max_x");
        debug_assert!(min_y <= max_y, "min_y was larger than max_y");
        Self {
            srid: srid,
            lower_left: GeoPoint::new(min_x, min_y),
            upper_right: GeoPoint::new(max_x, max_y),
        }
    }
    /// Returns the coordinates of the bounding box as a tuple.
    /// The values of the tuple correspond to:
    /// (min_x, min_y, max_x, max_y)
    pub fn as_tuple(&self) -> (f64, f64, f64, f64) {
        let (min_x, min_y) = self.lower_left.as_tuple();
        let (max_x, max_y) = self.upper_right.as_tuple();
        (min_x, min_y, max_x, max_y)
    }
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
