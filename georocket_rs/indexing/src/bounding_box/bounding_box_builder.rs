use super::BoundingBox;
use super::BoundingBoxError;
use super::GeoPoint;

#[derive(Debug)]
pub struct BoundingBoxBuilder {
    inner: Inner,
}

#[derive(Debug)]
enum Inner {
    Point {
        x: f64,
        y: f64,
    },
    BBoxPoints {
        min_x: f64,
        max_x: f64,
        min_y: f64,
        max_y: f64,
    },
    Uninitialized,
}

impl TryFrom<Inner> for Option<BoundingBox> {
    type Error = BoundingBoxError;

    fn try_from(inner: Inner) -> Result<Self, Self::Error> {
        if inner.validate() {
            Ok(match inner {
                Inner::Point { x, y } => Some(BoundingBox::Point(GeoPoint { x, y })),
                Inner::BBoxPoints {
                    min_x,
                    max_x,
                    min_y,
                    max_y,
                } => Some(BoundingBox::Box([
                    GeoPoint { x: min_x, y: min_y },
                    GeoPoint { x: max_x, y: min_y },
                    GeoPoint { x: max_x, y: max_y },
                    GeoPoint { x: min_x, y: max_y },
                    GeoPoint { x: min_x, y: min_y },
                ])),
                Inner::Uninitialized => None,
            })
        } else {
            Err(inner.into())
        }
    }
}

impl From<Inner> for BoundingBoxError {
    fn from(bbox: Inner) -> Self {
        debug_assert!(
            !matches!(bbox, Inner::Uninitialized),
            "BoundingBoxError should not be generated from `Inner::Uninitialized`"
        );
        match bbox {
            Inner::Point { x, y } => Self::InvalidCoordinates {
                min_x: x,
                min_y: y,
                max_x: x,
                max_y: y,
            },
            Inner::BBoxPoints {
                min_x,
                max_x,
                min_y,
                max_y,
            } => Self::InvalidCoordinates {
                min_x,
                min_y,
                max_x,
                max_y,
            },
            Inner::Uninitialized => unreachable!(
                "BoundingBoxError should not be generated from `BoundingBoxBuilder::Uninitialized`"
            ),
        }
    }
}

impl Inner {
    fn validate(&self) -> bool {
        let valid_point = |x, y| (x >= -180. && x <= 180.) && (y >= -90. && y <= 90.);
        match *self {
            Self::Point { x, y } => valid_point(x, y),
            Self::BBoxPoints {
                min_x,
                max_x,
                min_y,
                max_y,
            } => valid_point(min_x, min_y) && valid_point(max_x, max_y),
            Self::Uninitialized => true,
        }
    }
    fn add_point(self, x: f64, y: f64) -> Self {
        match self {
            Self::Uninitialized => Self::Point { x, y },
            Self::Point { x: px, y: py } => {
                let (min_x, max_x) = if x < px { (x, px) } else { (px, x) };
                let (min_y, max_y) = if y < py { (y, py) } else { (py, y) };
                Self::BBoxPoints {
                    min_x,
                    max_x,
                    min_y,
                    max_y,
                }
            }
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

impl BoundingBoxBuilder {
    pub fn new() -> Self {
        Self {
            inner: Inner::Uninitialized,
        }
    }
    pub fn validate(&self) -> bool {
        self.inner.validate()
    }
    pub fn build(self) -> Result<Option<BoundingBox>, BoundingBoxError> {
        self.inner.try_into()
    }
    pub fn add_point(self, x: f64, y: f64) -> Self {
        Self {
            inner: self.inner.add_point(x, y),
        }
    }
}
