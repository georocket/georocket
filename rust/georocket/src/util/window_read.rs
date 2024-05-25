use std::{
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;
use tokio::io::{AsyncRead, ReadBuf};

use super::window::Window;

/// Wrapper around an `AsyncRead` object. Buffers all bytes read in an internal
/// buffer enabling the extraction of relevant bytes
#[pin_project]
pub struct WindowRead<R> {
    #[pin]
    inner: R,
    window: Window,
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
        this.window.extend(&buf.filled()[old_length..new_length]);
        result
    }
}

impl<R> WindowRead<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            window: Default::default(),
        }
    }

    pub fn window(&self) -> &Window {
        &self.window
    }

    pub fn window_mut(&mut self) -> &mut Window {
        &mut self.window
    }
}

#[cfg(test)]
mod tests {
    use super::WindowRead;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    /// Read from a cursor and compare the full contents of the window
    #[tokio::test]
    async fn full() {
        // wrap WindowRead around a Cursor
        let data = "Hello world!".to_string();
        let cursor = Cursor::new(data);
        let mut wr = WindowRead::new(cursor);

        // read the full contents from the cursor
        let mut buf = Vec::new();
        wr.read_to_end(&mut buf).await.unwrap();

        // compare contents
        let window_buf = wr.window_mut().get_bytes(0..buf.len()).unwrap();
        assert_eq!(window_buf, buf);
    }

    /// Compare a range of bytes
    #[tokio::test]
    async fn range() {
        // wrap WindowRead around a Cursor
        let data = "Hello world!".to_string();
        let cursor = Cursor::new(data);
        let mut wr = WindowRead::new(cursor);

        // Read the full contents from the cursor. This will also fill the window.
        let mut buf = Vec::new();
        wr.read_to_end(&mut buf).await.unwrap();

        // Advance the window to the absolute position 6. This will remove the
        // first 6 bytes from the window.
        wr.window_mut().advance_to(6).unwrap();

        // Anything from before the start of the window cannot be accessed any more
        assert!(wr.window().get_bytes(0..4).is_err());

        // Read the next 5 bytes. Positions are still absolute!
        let window_buf = wr.window().get_bytes(6..11).unwrap();
        assert_eq!(window_buf, "world".as_bytes());

        // read the remainder
        let window_buf = wr.window().get_bytes(11..12).unwrap();
        assert_eq!(window_buf, "!".as_bytes());
    }
}
