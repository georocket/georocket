use anyhow::Result;
use std::collections::vec_deque::Iter;
use std::collections::VecDeque;
use std::iter::{Skip, Take};

const EXTEND: usize = 1024;

/// Buffers the bytes that pass through the splitter.
pub struct Buffer {
    inner: VecDeque<u8>,
    //marks the beginning of an object of interest
    marker: usize,
    consumed: usize,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            inner: VecDeque::with_capacity(EXTEND),
            marker: 0,
            consumed: 0,
        }
    }

    pub fn set_marker(&mut self, position: usize) {
        self.marker = position;
    }

    /// retrieves the bytes starting at the marker
    pub fn retrieve_marked(&mut self, length: usize) -> Vec<u8> {
        self.inner
            .iter()
            .skip(self.marker)
            .take(length)
            .cloned()
            .collect()
    }

    /// consumes all bytes from the start of the buffer, to the marker + length
    pub fn drain_marked(&mut self, length: usize) {
        self.inner.drain(0..self.marker + length);
        self.consumed -= self.marker + length;
        self.marker = 0;
    }

    pub fn fill_bytes(&mut self, bytes: &[u8]) {
        self.inner.extend(bytes);
    }

    pub fn fill_byte(&mut self, byte: u8) {
        self.inner.push_back(byte);
    }

    pub fn get_bytes(&mut self, count: usize) -> (&[u8], &[u8]) {
        let (front, back) = self.inner.as_slices();
        let start = self.consumed;
        let count = usize::min(self.remaining(), count);
        Self::map_range_to_slices(front, back, start, count)
    }

    fn map_range_to_slices<'a>(
        front: &'a [u8],
        back: &'a [u8],
        start: usize,
        count: usize,
    ) -> (&'a [u8], &'a [u8]) {
        let begins_in_front = start < front.len();
        let ends_in_front = start + count <= front.len();
        debug_assert!(
            {
                let remaining_bytes_in_slices = front.len() + back.len() - start;
                count <= remaining_bytes_in_slices
            },
            "Attempting to access {} bytes, however there are only {} bytes in the slices, starting from start {}",
            count, front.len() + back.len() - start, start
        );
        match (begins_in_front, ends_in_front) {
            (true, true) => (&front[start..start + count], &[]),
            (true, false) => {
                let remaining = count - front.len();
                (&front[start..], &back[..remaining])
            }
            (false, _) => {
                let start = start - front.len();
                (&[], &back[start..start + count])
            }
        }
    }
    // returns an iterator over the next `count` bytes.
    pub fn get_bytes_iter(&mut self, count: usize) -> Take<Skip<Iter<u8>>> {
        let iter = self.inner.iter().skip(self.consumed).take(count);
        self.consumed = self.inner.len().min(self.consumed + count);
        iter
    }

    // returns the next byte, consuming it
    pub fn get_byte(&mut self) -> Option<u8> {
        if self.consumed == self.inner.len() {
            None
        } else {
            let byte = self.inner[self.consumed];
            self.consumed += 1;
            Some(byte)
        }
    }

    /// Drops all bytes that have been consumed, effectively clearing the buffer.
    /// Resets the marker
    pub(crate) fn reset(&mut self, count: usize) {
        self.inner.drain(0..count);
        self.consumed -= count;
        self.marker = 0;
    }

    // returns true, if the buffer has consumed all of its buffered bytes
    pub fn end(&mut self) -> bool {
        self.inner.len() == self.consumed
    }

    fn remaining(&self) -> usize {
        self.inner.len() - self.consumed
    }

    pub fn consume(&mut self, count: usize) {
        debug_assert!(
            (count <= self.remaining()),
            "Attempted to consume {} bytes, but only {} remaining",
            count,
            self.remaining()
        );
        self.consumed += count;
    }
}

impl Iterator for Buffer {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_byte()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn retrieve_string() {
        let test_data = "xxxxTHISxxxx".as_bytes();
        let mut buffer = Buffer::new();
        buffer.fill_bytes(test_data);
        buffer.set_marker(4);
        assert_eq!(buffer.retrieve_marked(4).as_slice(), "THIS".as_bytes());
    }

    #[test]
    fn access_buffer_iter() {
        let mut buffer = Buffer::new();
        (0..10).for_each(|byte| {
            buffer.fill_byte(byte);
        });
        assert_eq!(0, buffer.get_byte().unwrap());
        assert_eq!(1, buffer.get_byte().unwrap());
        assert_eq!(2, buffer.get_byte().unwrap());
        assert_eq!(3, buffer.get_byte().unwrap());
        let mut control = 4;
        buffer.get_bytes_iter(10).for_each(|byte| {
            assert_eq!(*byte, control);
            control += 1;
        });
        assert_eq!(buffer.consumed, 10);
        assert_eq!(control, 10);
        assert_eq!(None, buffer.get_byte())
    }

    #[test]
    fn access_buffer_slices() {
        let mut buffer = Buffer::new();
        (0..10).for_each(|byte| {
            buffer.fill_byte(byte);
        });
        assert_eq!(0, buffer.get_byte().unwrap());
        assert_eq!(1, buffer.get_byte().unwrap());
        assert_eq!(2, buffer.get_byte().unwrap());
        assert_eq!(3, buffer.get_byte().unwrap());
        let mut control = 4;
        let (first, second) = buffer.get_bytes(10);
        let bytes_retrieved = first.len() + second.len();
        control += bytes_retrieved;
        buffer.consume(bytes_retrieved);
        assert_eq!(buffer.consumed, 10);
        assert_eq!(control, 10);
        assert_eq!(None, buffer.get_byte())
    }

    #[test]
    fn reset_buffer() {
        let mut buffer = Buffer::new();
        (0..10).for_each(|byte| {
            buffer.fill_byte(byte);
        });
        let _ = buffer.get_bytes_iter(4);
        buffer.set_marker(4);
        buffer.reset(4);
        assert_eq!(buffer.inner.len(), 6);
        assert_eq!(buffer.consumed, 0);
        assert_eq!(buffer.marker, 0);
        for (x, y) in buffer.zip(4..10) {
            assert_eq!(x, y)
        }
    }

    #[test]
    fn empty_buffer() {
        let mut buffer = Buffer::new();
        assert!(buffer.end());
        (0..10).for_each(|byte| {
            buffer.fill_byte(byte);
        });
        let _ = buffer.get_bytes_iter(9);
        assert!(!buffer.end());
        buffer.get_byte();
        assert!(buffer.end())
    }

    #[test]
    fn range_from_slices() {
        let front = [1, 2, 3];
        let back = [4, 5, 6];
        //start and end in front
        let (first, second) = Buffer::map_range_to_slices(&front, &back, 0, 3);
        assert_eq!(first, &[1, 2, 3]);
        assert_eq!(second, &[]);

        //start in front, end in back
        let (first, second) = Buffer::map_range_to_slices(&front, &back, 0, 5);
        assert_eq!(first, &[1, 2, 3]);
        assert_eq!(second, &[4, 5]);

        //start in back
        let (first_a, second_a) = Buffer::map_range_to_slices(&front, &back, 4, 2);
        assert_eq!(first_a, &[]);
        assert_eq!(second_a, &[5, 6]);

        //same, but attempting to take more than is contained
    }
}
