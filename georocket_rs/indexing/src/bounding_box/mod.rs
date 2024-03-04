//! The `bounding_box` module contains the [`BoundingBoxBuilder`] as well as the [`Validator`] trait.
//! The `BoundingBoxBuilder` can be used when creating bounding boxes in an iterative fashion,
//! as it allows adding points as they become available. It will also validate the bounding box and
//! return an appropriate error based off of the [`Validator`] implementation the `BoundingBoxBuilder`
//! is generic over.
//! Custom validation logic can be implemented by implementing the [`Validator`] trait.
//!
//! # Multiple Points
//!
//! ```
//! # use indexing::bounding_box::*;
//! # use indexing::bounding_box::NoValidation;
//! # use georocket_types::{BoundingBox, GeoPoint};
//! let mut bbox_builder = BoundingBoxBuilder::new(NoValidation);
//! bbox_builder = bbox_builder.set_srid(4326);
//! bbox_builder = bbox_builder.add_point(42.0, 1.618);
//! // calculate or retrieve further points...
//! bbox_builder = bbox_builder.add_point(3.141, 69.0);
//! // calculate or retrieve further points...
//! let Ok(Some(bbox)) = bbox_builder.build() else { panic!("box wasn't okay") };
//! assert_eq!(bbox,
//!            BoundingBox{
//!                 srid: Some(4326),
//!                 lower_left: GeoPoint::new(3.141, 1.618),
//!                 upper_right: GeoPoint::new(42.0, 69.0),
//!             });
//! ```
//!
//! # No Points
//!
//! If no points have been provided to the `BoundingBoxBuildr`, it will return
//! `None`:
//!
//! ```
//! # use indexing::bounding_box::*;
//! let mut bbox_builder = BoundingBoxBuilder::new(NoValidation);
//! assert!(bbox_builder.build().is_ok_and(|bbox| bbox.is_none()));
//! ```
//!
//! # SRID
//!
//! The `BoundingBoxBuilder` can be set with an SRID. If no SRID is set, it will default to `None`.
//! The SRID can be set using the [`set_srid`](BoundingBoxBuilder::set_srid) method:
//!
//! ```
//! # use indexing::bounding_box::*;
//! # use indexing::bounding_box::NoValidation;
//! let mut bbox_builder = BoundingBoxBuilder::new(NoValidation);
//! bbox_builder = bbox_builder.set_srid(4326).add_point(42.0, 1.618);
//! let Ok(Some(bbox)) = bbox_builder.build() else { panic!("box wasn't okay") };
//! assert_eq!(bbox.srid, Some(4326))
//! ```
//!
//! # Errors
//! The [`BoundingBoxBuilder::build`] method can error. It will error if the [`Validator::validate`]
//! method returns an error when attempting to validate the bounding box.
//!
//! ```
//! # use indexing::bounding_box::*;
//! # use indexing::bounding_box::wgs84_validator::{WGS84BuilderError, WGS84Validator};
//! let mut bbox_builder = BoundingBoxBuilder::new(WGS84Validator).add_point(192., 98.,);
//! assert_eq!(
//!     bbox_builder.build().unwrap_err(),
//!     WGS84BuilderError::InvalidCoordinates {
//!         min_x: 192.,
//!         min_y: 98.,
//!         max_x: 192.,
//!         max_y: 98.,
//!     }
//! )
//! ```

use georocket_types::BoundingBox;
use georocket_types::GeoPoint;

mod bounding_box_builder;
pub mod wgs84_validator;

pub use bounding_box_builder::BoundingBoxBuilder;
pub use bounding_box_builder::NoValidation;
pub use bounding_box_builder::Validator;
