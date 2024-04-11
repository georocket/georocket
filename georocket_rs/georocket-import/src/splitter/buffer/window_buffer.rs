use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Range;

#[derive(Debug)]
pub enum WindowBufferError {
    BytesOutOfRange,
    AdvanceIndexOutOfRange,
}

impl Display for WindowBufferError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowBufferError::BytesOutOfRange => {
                write!(
                    f,
                    "specified bytes are outside of the current range of the window"
                )
            }
            WindowBufferError::AdvanceIndexOutOfRange => {
                write!(
                    f,
                    "specified index outside of the current range of the window"
                )
            }
        }
    }
}

impl Error for WindowBufferError {}

const EXTEND: usize = 1024;

/// Represents a sliding window over a stream of bytes.
/// The type provides convenience functions for accessing ranges of bytes based on their absolute
/// position in
pub struct WindowBuffer {
    /// The current contents of the window
    inner: VecDeque<u8>,
    /// The offset from the beginning of the byte stream to the current beginning of the
    /// bytes buffered in the window
    offset: usize,
}

impl WindowBuffer {
    /// Creates a new `WindowBuffer`.
    pub fn new() -> Self {
        Self::with_capacity(EXTEND)
    }
    /// Creates a new `WindowBuffer` with the specified `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
            offset: 0,
        }
    }
    /// Copies the supplied bytes into the window.
    pub fn fill_bytes(&mut self, bytes: &[u8]) {
        self.inner.extend(bytes)
    }
    /// Retrieves the specified range of bytes.
    /// # Errors:
    /// Returns `WindowBufferError::BytesOutOfRange` error, if the specified range
    /// falls outside the range of bytes buffered by the window.
    pub fn get_bytes<'w>(
        &self,
        range: Range<usize>,
    ) -> Result<impl ExactSizeIterator<Item = &u8>, WindowBufferError> {
        let window_end = self.offset + self.inner.len();
        if range.start < self.offset || range.start > window_end || range.end > window_end {
            return Err(WindowBufferError::BytesOutOfRange);
        }
        let begin = range.start - self.offset;
        let amount = range.end - range.start;
        Ok(self.inner.iter().skip(begin).take(amount))
    }
    /// Retrieves the specified range of bytes as a `Vec<u8>`.
    /// # Errors:
    ///
    /// Returns `WindowBufferError::BytesOutOfRange` error, if the specified range
    /// falls outside the range of bytes buffered by the window.
    pub fn get_bytes_vec(&self, range: Range<usize>) -> Result<Vec<u8>, WindowBufferError> {
        Ok(self.get_bytes(range)?.cloned().collect())
    }
    /// Retrieves all bytes contained within the window, starting from the specified position.
    /// # Errors:
    /// Returns `WindowBufferError::BytesOutOfRange` error, if the starting position
    /// falls outside the range of bytes buffered by the window.
    pub fn get_bytes_from<'w>(
        &self,
        start: usize,
    ) -> Result<impl ExactSizeIterator<Item = &u8>, WindowBufferError> {
        let window_end = self.offset + self.inner.len();
        if start < self.offset || start > window_end {
            return Err(WindowBufferError::BytesOutOfRange);
        }
        let begin = start - self.offset;
        Ok(self.inner.iter().skip(begin))
    }
    /// Retrieves all bytes contained within the window.
    pub fn get_bytes_all<'w>(&self) -> impl ExactSizeIterator<Item = &u8> {
        self.inner.iter()
    }
    /// Drains all bytes up to `idx` out of the window, where `idx` is an absolute position
    /// in the byte stream, effectively moving the window forward over the byte-stream.
    /// Does nothing if `idx` is smaller than the current offset of the window to the beginning
    /// of the byte stream.
    /// # Errors:
    /// Returns `WindowBufferError::AdvanceIndexOutOfRange` if `idx` exceeds any position beyond the
    /// end of the window or is smaller than the current offset of the window.
    pub fn move_window(&mut self, idx: usize) -> Result<(), WindowBufferError> {
        if idx > self.offset + self.inner.len() || idx < self.offset {
            return Err(WindowBufferError::AdvanceIndexOutOfRange);
        }
        let diff = idx - self.offset;
        self.inner.drain(0..diff);
        self.offset = idx;
        Ok(())
    }
    /// Returns the current range of the byte-stream that is covered by the window
    pub fn active_range(&self) -> Range<usize> {
        self.offset..self.offset + self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn simple_retreive() {
        let bytes: Vec<u8> = (0..=u8::MAX).collect();
        let mut buffer = WindowBuffer::new();
        buffer.fill_bytes(&bytes);
        let range = 0..20;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);

        let range = 13..67;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);

        let range = 184..256;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);

        let range = 0..256;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);
    }

    #[test]
    fn simple_slide() {
        let bytes: Vec<u8> = (0..=u8::MAX).collect();
        let mut buffer = WindowBuffer::new();
        // fill some bytes into the window and then advance the window
        buffer.fill_bytes(&bytes[0..20]);
        buffer.move_window(5).unwrap();
        // accessing bytes within the current active window should work as if slicing into the
        // original byte-stream.
        // Current range in window: 5..20
        assert_eq!(buffer.active_range(), 5..20);
        let range = 8..18;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);

        let range = 5..20;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);

        // accessing range outside the active range should error
        let range = 4..20;
        assert!(buffer.get_bytes(range).is_err());
        let range = 4..21;
        assert!(buffer.get_bytes(range).is_err());

        // fill some more bytes into the window
        buffer.fill_bytes(&bytes[20..40]);
        // Current range in window: 5..40
        assert_eq!(buffer.active_range(), 5..40);

        let range = 5..40;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);
    }

    #[test]
    fn get_bytes_from() {
        let bytes: Vec<u8> = (0..=u8::MAX).collect();
        let mut buffer = WindowBuffer::new();
        // fill some bytes into the window and then advance the window
        buffer.fill_bytes(&bytes[0..20]);
        buffer.move_window(5).unwrap();
        // accessing bytes within the current active window should work as if slicing into the
        // original byte-stream.
        // Current range in window: 5..20
        assert_eq!(buffer.active_range(), 5..20);
        let range = 8..18;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);

        let range = 5..20;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);

        // accessing range outside the active range should error
        let range = 4..20;
        assert!(buffer.get_bytes(range).is_err());
        let range = 4..21;
        assert!(buffer.get_bytes(range).is_err());

        // fill some more bytes into the window
        buffer.fill_bytes(&bytes[20..40]);
        // Current range in window: 5..40
        assert_eq!(buffer.active_range(), 5..40);

        let range = 5..40;
        let slice = &bytes[range.clone()];
        let retrieved: Vec<u8> = buffer.get_bytes(range).unwrap().cloned().collect();
        assert_eq!(slice, retrieved);
    }
}
