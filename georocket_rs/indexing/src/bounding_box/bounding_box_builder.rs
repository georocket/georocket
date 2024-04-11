use std::error::Error;

use super::BoundingBox;
use super::GeoPoint;

/// A trait for validating coordinates.
pub trait Validator {
    type Error: Error;
    /// Validates if the given coordinates are valid according to the implemented standard.
    fn validate(&self, geo_point: GeoPoint, upper_right: GeoPoint) -> Result<(), Self::Error>;
}

/// A validator that does not perform any validation.
#[derive(Debug, Copy, Clone)]
pub struct NoValidation;

impl Validator for NoValidation {
    type Error = std::convert::Infallible;
    fn validate(&self, _: GeoPoint, _: GeoPoint) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A builder to create [`BoundingBox`es](BoundingBox).
///
/// The Builder takes x and y coordinates to specify the `BoundingBox`.
/// It will return `None`, if no coordinates have been provided.
///
/// The builder is generic over a `Validator` which can be used to validate the coordinates.
#[derive(Debug, Copy, Clone)]
pub struct BoundingBoxBuilder<V>
where
    V: Validator,
{
    inner: Inner,
    srid: Option<u32>,
    validator: V,
}

impl<V> BoundingBoxBuilder<V>
where
    V: Validator,
{
    /// Creates a new `BoundingBoxBuilder`.
    pub fn new(validator: V) -> Self {
        Self {
            inner: Inner::Uninitialized,
            srid: None,
            validator,
        }
    }
    /// If all coordinates passed into the builder were valid, calling `build()`
    /// returns `Ok(Some(BoundingBox))`. If no coordinates were passed into the
    /// builder, it returns `Ok(None)`.
    ///
    /// # Errors
    /// If one or more of the passed in coordinates were invalid, `build` returns
    /// a [`InvalidCoordinates`](WGS84BuilderError::InvalidCoordinates) error.
    pub fn build(self) -> Result<Option<BoundingBox>, V::Error> {
        Ok(match self.inner {
            Inner::BBoxPoints {
                min_x,
                max_x,
                min_y,
                max_y,
            } => {
                let lower_left = GeoPoint { x: min_x, y: min_y };
                let upper_right = GeoPoint { x: max_x, y: max_y };
                self.validator.validate(lower_left, upper_right)?;
                Some(BoundingBox {
                    srid: self.srid,
                    lower_left,
                    upper_right,
                })
            }
            Inner::Uninitialized => None,
        })
    }
    /// Adds a coordinate point to the builder. No validation is done in this method,
    /// instead [`build`](BoundingBoxBuilder::build) does validation when called.
    #[must_use]
    pub fn add_point(self, x: f64, y: f64) -> Self {
        Self {
            inner: self.inner.add_point(x, y),
            ..self
        }
    }
    /// Adds a SRID to the builder.
    #[must_use]
    pub fn set_srid(self, srid: u32) -> Self {
        Self {
            srid: Some(srid),
            ..self
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum Inner {
    BBoxPoints {
        min_x: f64,
        max_x: f64,
        min_y: f64,
        max_y: f64,
    },
    Uninitialized,
}

impl Inner {
    fn add_point(self, x: f64, y: f64) -> Self {
        match self {
            Self::Uninitialized => Self::BBoxPoints {
                min_x: x,
                max_x: x,
                min_y: y,
                max_y: y,
            },
            Self::BBoxPoints {
                min_x,
                max_x,
                min_y,
                max_y,
            } => Self::BBoxPoints {
                min_x: if x < min_x { x } else { min_x },
                min_y: if y < min_y { y } else { min_y },
                max_y: if y > max_y { y } else { max_y },
                max_x: if x > max_x { x } else { max_x },
            },
        }
    }
}
