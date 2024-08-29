use geo::Rect;

/// Trait to extend rectangles
///
/// # Examples
///
/// ```rust
/// use geo::{coord, Rect};
/// use georocket_core::util::extend_rect::ExtendRect;
///
/// let mut bb = Rect::new(
///     coord! { x: 1.0, y: 2.0 },
///     coord! { x: 1.0, y: 2.0 }
/// );
/// assert_eq!(bb.min().x, 1.0);
/// assert_eq!(bb.min().y, 2.0);
/// assert_eq!(bb.max().x, 1.0);
/// assert_eq!(bb.max().y, 2.0);
///
/// bb.extend_point(5.0, 6.0);
/// assert_eq!(bb.min().x, 1.0);
/// assert_eq!(bb.min().y, 2.0);
/// assert_eq!(bb.max().x, 5.0);
/// assert_eq!(bb.max().y, 6.0);
///
/// bb.extend_point(-7.0, -6.0);
/// assert_eq!(bb.min().x, -7.0);
/// assert_eq!(bb.min().y, -6.0);
/// assert_eq!(bb.max().x, 5.0);
/// assert_eq!(bb.max().y, 6.0);
/// ```
///
/// ```rust
/// use geo::{coord, Rect};
/// use georocket_core::util::extend_rect::ExtendRect;
///
/// let mut bb1 = Rect::new(
///     coord! { x: 1.0, y: 2.0 },
///     coord! { x: 4.0, y: 5.0 }
/// );
/// let mut bb2 = Rect::new(
///     coord! { x: 40.0, y: 50.0 },
///     coord! { x: 70.0, y: 80.0 }
/// );
///
/// bb1.extend_rect(&bb2);
/// assert_eq!(bb1.min().x, 1.0);
/// assert_eq!(bb1.min().y, 2.0);
/// assert_eq!(bb1.max().x, 70.0);
/// assert_eq!(bb1.max().y, 80.0);
///
/// bb2.extend_rect(&bb1);
/// assert_eq!(bb2.min().x, 1.0);
/// assert_eq!(bb2.min().y, 2.0);
/// assert_eq!(bb2.max().x, 70.0);
/// assert_eq!(bb2.max().y, 80.0);
/// ```
pub trait ExtendRect {
    /// Extends the rectangle so it overlaps the given point
    fn extend_point(&mut self, x: f64, y: f64);

    /// Extends the rectangle so it overlaps the given other rectangle
    fn extend_rect(&mut self, other: &Rect);
}

impl ExtendRect for Rect {
    fn extend_point(&mut self, x: f64, y: f64) {
        let min = self.min();
        self.set_min((min.x.min(x), min.y.min(y)));
        let max = self.max();
        self.set_max((max.x.max(x), max.y.max(y)));
    }

    fn extend_rect(&mut self, other: &Rect) {
        let min = self.min();
        let other_min = other.min();
        self.set_min((min.x.min(other_min.x), min.y.min(other_min.y)));
        let max = self.max();
        let other_max = other.max();
        self.set_max((max.x.max(other_max.x), max.y.max(other_max.y)));
    }
}
