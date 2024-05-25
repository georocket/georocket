use anyhow::{bail, Result};
use std::{collections::VecDeque, ops::Range};

/// A dynamically resizable buffer that acts like a window being moved over
/// a larger input stream
#[derive(Default)]
pub struct Window {
    pos: usize,
    buf: VecDeque<u8>,
}

impl Window {
    /// Append data to the window (i.e. make it larger)
    pub fn extend(&mut self, buf: &[u8]) {
        self.buf.extend(buf);
    }

    /// Return a chunk from the window using a given [`Range`]. The range
    /// specifies byte positions that are absolute to the larger input stream
    /// the window is being moved over.
    pub fn get_bytes(&self, range: Range<usize>) -> Result<Vec<u8>> {
        if range.is_empty() {
            return Ok(Vec::new());
        }

        if self.pos > range.start || self.pos > range.end {
            bail!("Unable to get bytes from before the start of the window");
        }

        if self.pos + self.buf.len() <= range.start || self.pos + self.buf.len() < range.end {
            bail!("Unable to get bytes from beyond the end of the window");
        }

        let start = range.start - self.pos;
        let end = range.end - self.pos;

        let r = self
            .buf
            .iter()
            .skip(start)
            .take(end - start)
            .copied()
            .collect();

        Ok(r)
    }

    /// Moves the window's start to the given position. The position is absolute
    /// to the larger input stream the window is being moved over. The window's
    /// end will not be moved, i.e. this function will remove bytes from the
    /// beginning of the window.
    pub fn advance_to(&mut self, pos: usize) -> Result<()> {
        if pos < self.pos {
            bail!("Unable to advance to a position before the current start of the window");
        }

        if pos > self.pos + self.buf.len() {
            bail!("Unable to advance to a position beyond the current end of the window");
        }

        self.buf.drain(0..pos - self.pos);
        self.pos = pos;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Window;

    /// Check if an empty window behaves correctly
    #[test]
    fn empty() {
        let mut w = Window::default();
        assert_eq!(w.get_bytes(0..0).unwrap(), &[]);
        assert!(w.get_bytes(0..1).is_err());
        assert!(w.get_bytes(1..2).is_err());
        assert!(w.advance_to(10).is_err());
    }

    /// Get full contents of the window
    #[test]
    fn full() {
        let data = "Hello".as_bytes();
        let mut w = Window::default();
        w.extend(data);
        let contents = w.get_bytes(0..5).unwrap();
        assert_eq!(data, contents);
    }

    /// Test range checks
    #[test]
    fn range_checks() {
        let data = "Hello".as_bytes();
        let mut w = Window::default();
        w.extend(data);

        assert_eq!(w.get_bytes(0..0).unwrap(), &[]);
        assert_eq!(w.get_bytes(1..1).unwrap(), &[]);

        assert_eq!(w.get_bytes(1..2).unwrap(), &[b'e']);
        assert_eq!(w.get_bytes(2..4).unwrap(), &[b'l', b'l']);

        assert!(w.get_bytes(6..10).is_err());
        assert!(w.advance_to(10).is_err());

        w.advance_to(3).unwrap();

        assert!(w.advance_to(0).is_err());
        assert!(w.advance_to(1).is_err());
        assert!(w.advance_to(2).is_err());

        w.advance_to(3).unwrap();

        assert_eq!(w.get_bytes(3..4).unwrap(), &[b'l']);
        assert_eq!(w.get_bytes(4..5).unwrap(), &[b'o']);
        assert_eq!(w.get_bytes(3..5).unwrap(), &[b'l', b'o']);
        assert!(w.get_bytes(0..1).is_err());
        assert!(w.get_bytes(0..2).is_err());
        assert!(w.get_bytes(2..3).is_err());
        assert!(w.get_bytes(5..10).is_err());
        assert!(w.get_bytes(6..10).is_err());
        assert!(w.advance_to(10).is_err());

        w.advance_to(4).unwrap();
        w.advance_to(5).unwrap();
        assert!(w.advance_to(6).is_err());
    }
}
