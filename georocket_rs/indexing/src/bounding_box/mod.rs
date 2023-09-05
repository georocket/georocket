use std::error::Error;
mod bounding_box_builder;
pub use bounding_box_builder::BoundingBoxBuilder;

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum BoundingBoxError {
    InvalidCoordinates {
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    },
}

impl Error for BoundingBoxError {}

impl Display for BoundingBoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundingBoxError::InvalidCoordinates {
                min_x,
                min_y,
                max_x,
                max_y,
            } => write!(
                f,
                "invalid bounding box [{}, {}, {}, {}]. \
                Values outside [-180.0, -90.0, 180.0, 90.0]",
                min_x, min_y, max_x, max_y
            ),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
pub enum BoundingBox {
    Point(GeoPoint),
    Box([GeoPoint; 5]),
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    x: f64,
    y: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use BoundingBox as B;

    fn assert_valid_point(x: f64, y: f64) {
        let bbox = BoundingBoxBuilder::new()
            .add_point(x, y)
            .build()
            .unwrap()
            .unwrap();
        assert_eq!(bbox, B::Point(GeoPoint { x, y }));
    }

    fn assert_out_of_bound_point(x: f64, y: f64) {
        let bbox = BoundingBoxBuilder::new().add_point(x, y).build();
        assert_eq!(
            bbox.unwrap_err(),
            BoundingBoxError::InvalidCoordinates {
                min_x: x,
                min_y: y,
                max_x: x,
                max_y: y,
            }
        )
    }

    #[test]
    fn empty_bounding_box() {
        let bbox = BoundingBoxBuilder::new().build();
        assert!(bbox.is_ok_and(|b| b.is_none()));
    }

    #[test]
    fn points() {
        let valid_points = [(-70., -64.), (70., -64.), (-70., 64.), (70., 64.)];
        let edgecases = [
            (-180., -90.),
            (180., -90.),
            (-180., 90.),
            (180., 90.),
            (0., 0.),
        ];
        let points = valid_points.into_iter().chain(edgecases.into_iter());
        for (x, y) in points {
            assert_valid_point(x, y)
        }
    }

    #[test]
    fn points_out_of_bounds() {
        let out_of_bounds_points = [(-190., -64.), (-42., -100.), (190., 42.), (100., 233.)];
        out_of_bounds_points
            .iter()
            .cloned()
            .for_each(|(x, y)| assert_out_of_bound_point(x, y));
    }

    fn get_min_and_max(polygon: &[(f64, f64)]) -> (f64, f64, f64, f64) {
        assert!(!polygon.is_empty());
        let (min_x, _) = polygon
            .iter()
            .min_by(|(x, _), (x_min, _)| x.total_cmp(x_min))
            .unwrap()
            .to_owned();
        let (max_x, _) = polygon
            .iter()
            .max_by(|(x, _), (x_max, _)| x.total_cmp(x_max))
            .unwrap()
            .to_owned();
        let (_, min_y) = polygon
            .iter()
            .min_by(|(_, y), (_, y_min)| y.total_cmp(y_min))
            .unwrap()
            .to_owned();
        let (_, max_y) = polygon
            .iter()
            .max_by(|(_, y), (_, y_max)| y.total_cmp(y_max))
            .unwrap()
            .to_owned();
        (min_x, max_x, min_y, max_y)
    }

    #[test]
    fn test_get_min_and_max() {
        let polygon = [(-132., 89.), (32., -42.), (234., 32.), (13., 34900.)];
        let (min_x, max_x, min_y, max_y) = get_min_and_max(&polygon);
        assert_eq!(-132., min_x);
        assert_eq!(234., max_x);
        assert_eq!(-42., min_y);
        assert_eq!(34900., max_y);
    }

    fn assert_valid_polygon(polygon: &[(f64, f64)]) {
        let mut bbox_builder = BoundingBoxBuilder::new();
        for (x, y) in polygon.iter().cloned() {
            bbox_builder = bbox_builder.add_point(x, y);
        }
        let bbox = bbox_builder.build().unwrap().unwrap();

        let (min_x, max_x, min_y, max_y) = get_min_and_max(&polygon);
        let control_box = BoundingBox::Box([
            GeoPoint { x: min_x, y: min_y },
            GeoPoint { x: max_x, y: min_y },
            GeoPoint { x: max_x, y: max_y },
            GeoPoint { x: min_x, y: max_y },
            GeoPoint { x: min_x, y: min_y },
        ]);
        assert_eq!(control_box, bbox);
    }

    #[test]
    fn valid_polygons() {
        let simple_polygon = [(-10., -10.), (10., 10.)];
        let complex_polygon = [(-23., 54.), (112., 0.), (-98., 9.)];
        assert_valid_polygon(&simple_polygon);
        assert_valid_polygon(&complex_polygon);
        let edgecase_polygons = [
            [(-180., -90.), (180., 90.)],
            [(180., -90.), (-180., 90.)],
            [(-180., 90.), (180., -90.)],
            [(180., 90.), (-180., -90.)],
        ];
        for polygon in edgecase_polygons {
            assert_valid_polygon(&polygon)
        }
    }

    fn assert_out_of_bounds_polygon(polygon: &[(f64, f64)]) {
        let mut bbox_builder = BoundingBoxBuilder::new();
        for (x, y) in polygon.iter().cloned() {
            bbox_builder = bbox_builder.add_point(x, y);
        }
        let bbox_error = bbox_builder.build().unwrap_err();

        let (min_x, max_x, min_y, max_y) = get_min_and_max(&polygon);
        let control_error = BoundingBoxError::InvalidCoordinates {
            min_x,
            min_y,
            max_x,
            max_y,
        };
        assert_eq!(control_error, bbox_error);
    }

    #[test]
    fn out_of_bounds_polygons() {
        let out_of_bounds_polygons = [
            [(181., 0.), (0., 0.)],
            [(0., 91.), (0., 0.)],
            [(0., 0.), (181., 0.)],
            [(0., 0.), (0., 91.)],
            [(-181., 0.), (0., 0.)],
            [(0., -91.), (0., 0.)],
            [(0., 0.), (-181., 0.)],
            [(0., 0.), (0., -91.)],
        ];
        for polygon in out_of_bounds_polygons {
            assert_out_of_bounds_polygon(&polygon)
        }
    }
}
