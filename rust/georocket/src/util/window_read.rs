use std::{
    collections::{vec_deque::Drain, VecDeque},
    ops::RangeBounds,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;
use tokio::io::{AsyncRead, ReadBuf};

/// Wrapper around an `AsyncRead` object. Buffers all bytes read in an internal
/// buffer enabling the extraction of relevant bytes
#[pin_project]
pub struct WindowRead<R> {
    #[pin]
    inner: R,
    buf: VecDeque<u8>,
}

impl<R: AsyncRead> AsyncRead for WindowRead<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let old_length = buf.filled().len();
        let result = this.inner.poll_read(cx, buf);
        let new_length = buf.filled().len();
        this.buf.extend(&buf.filled()[old_length..new_length]);
        result
    }
}

impl<R> WindowRead<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: VecDeque::new(),
        }
    }

    /// Removes the specified range of bytes from the inner buffer and returns
    /// all removed bytes as an iterator.
    fn drain<B>(&mut self, range: B) -> Drain<u8>
    where
        B: RangeBounds<usize>,
    {
        self.buf.drain(range)
    }
}

#[cfg(test)]
mod tests {
    use super::WindowRead;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn simple() {
        // wrap WindowRead around a Cursor
        let data = "Hello world!".to_string();
        let cursor = Cursor::new(data);
        let mut wr = WindowRead::new(cursor);

        // read the full contents from the cursor
        let mut buf = Vec::new();
        wr.read_to_end(&mut buf).await.unwrap();

        // compare contents
        let window_buf = wr.drain(..).collect::<Vec<_>>();
        assert_eq!(window_buf, buf);
    }

    #[tokio::test]
    async fn drain_range() {
        // wrap WindowRead around a Cursor
        let data = "Hello world!".to_string();
        let cursor = Cursor::new(data);
        let mut wr = WindowRead::new(cursor);

        // read the full contents from the cursor
        let mut buf = Vec::new();
        wr.read_to_end(&mut buf).await.unwrap();

        // skip the first 6 bytes
        wr.drain(0..6);

        // read the next 5 bytes
        let window_buf = wr.drain(0..5).collect::<Vec<_>>();
        assert_eq!(window_buf, "world".as_bytes());

        // read the remainder
        let window_buf = wr.drain(..).collect::<Vec<_>>();
        assert_eq!(window_buf, "!".as_bytes());
    }
}
