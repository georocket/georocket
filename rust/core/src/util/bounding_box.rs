/// An axis-aligned bounding box
///
/// # Examples
///
/// ```rust
/// use core::util::bounding_box::BoundingBox;
///
/// let mut bb = BoundingBox::from_point(1.0, 2.0, 3.0);
/// assert_eq!(bb.min_x, 1.0);
/// assert_eq!(bb.min_y, 2.0);
/// assert_eq!(bb.min_z, 3.0);
/// assert_eq!(bb.max_x, 1.0);
/// assert_eq!(bb.max_y, 2.0);
/// assert_eq!(bb.max_z, 3.0);
///
/// bb.extend_point(5.0, 6.0, 7.0);
/// assert_eq!(bb.min_x, 1.0);
/// assert_eq!(bb.min_y, 2.0);
/// assert_eq!(bb.min_z, 3.0);
/// assert_eq!(bb.max_x, 5.0);
/// assert_eq!(bb.max_y, 6.0);
/// assert_eq!(bb.max_z, 7.0);
///
/// bb.extend_point(-7.0, -6.0, -5.0);
/// assert_eq!(bb.min_x, -7.0);
/// assert_eq!(bb.min_y, -6.0);
/// assert_eq!(bb.min_z, -5.0);
/// assert_eq!(bb.max_x, 5.0);
/// assert_eq!(bb.max_y, 6.0);
/// assert_eq!(bb.max_z, 7.0);
/// ```
///
/// ```rust
/// use core::util::bounding_box::BoundingBox;
///
/// let mut bb1 = BoundingBox::new(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
/// let mut bb2 = BoundingBox::new(40.0, 50.0, 60.0, 70.0, 80.0, 90.0);
///
/// bb1.extend_bbox(&bb2);
/// assert_eq!(bb1.min_x, 1.0);
/// assert_eq!(bb1.min_y, 2.0);
/// assert_eq!(bb1.min_z, 3.0);
/// assert_eq!(bb1.max_x, 70.0);
/// assert_eq!(bb1.max_y, 80.0);
/// assert_eq!(bb1.max_z, 90.0);
///
/// bb2.extend_bbox(&bb1);
/// assert_eq!(bb2.min_x, 1.0);
/// assert_eq!(bb2.min_y, 2.0);
/// assert_eq!(bb2.min_z, 3.0);
/// assert_eq!(bb2.max_x, 70.0);
/// assert_eq!(bb2.max_y, 80.0);
/// assert_eq!(bb2.max_z, 90.0);
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BoundingBox {
    pub min_x: f64,
    pub min_y: f64,
    pub min_z: f64,
    pub max_x: f64,
    pub max_y: f64,
    pub max_z: f64,
}

impl BoundingBox {
    /// Creates a new bounding box with the given extent
    pub fn new(min_x: f64, min_y: f64, min_z: f64, max_x: f64, max_y: f64, max_z: f64) -> Self {
        Self {
            min_x,
            min_y,
            min_z,
            max_x,
            max_y,
            max_z,
        }
    }

    /// Creates a new bounding box from the given point. The box will have an
    /// area of 0.
    pub fn from_point(x: f64, y: f64, z: f64) -> Self {
        Self {
            min_x: x,
            min_y: y,
            min_z: z,
            max_x: x,
            max_y: y,
            max_z: z,
        }
    }

    /// Extends the bounding box so it overlaps the given point
    pub fn extend_point(&mut self, x: f64, y: f64, z: f64) {
        self.min_x = self.min_x.min(x);
        self.min_y = self.min_y.min(y);
        self.min_z = self.min_z.min(z);
        self.max_x = self.max_x.max(x);
        self.max_y = self.max_y.max(y);
        self.max_z = self.max_z.max(z);
    }

    /// Extends the bounding box so it overlaps the given other box
    pub fn extend_bbox(&mut self, other: &BoundingBox) {
        self.min_x = self.min_x.min(other.min_x);
        self.min_y = self.min_y.min(other.min_y);
        self.min_z = self.min_z.min(other.min_z);
        self.max_x = self.max_x.max(other.max_x);
        self.max_y = self.max_y.max(other.max_y);
        self.max_z = self.max_z.max(other.max_z);
    }
}
