use super::window_buffer::WindowBuffer;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// Wrapper around a Reader and a `WindowBuffer`.
/// Buffers all bytes read by the Reader in the window, enabling the extraction of relevant bytes.
#[pin_project]
pub struct ScratchReader<R> {
    #[pin]
    reader: R,
    buffer: WindowBuffer,
}

impl<R: AsyncRead> AsyncRead for ScratchReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let old_length = buf.filled().len();
        let result = this.reader.poll_read(cx, buf);
        let new_length = buf.filled().len();
        this.buffer
            .fill_bytes(&buf.filled()[old_length..new_length]);
        result
    }
}

impl<R> ScratchReader<R> {
    /// Creates a new `ScratchReader` from the provided reader.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: WindowBuffer::new(),
        }
    }
    /// Returns a mutable reference to the window for extracting bytes.
    pub fn window_mut(&mut self) -> &mut WindowBuffer {
        &mut self.buffer
    }
    /// Returns a reference to the window for extracting bytes.
    pub fn window(&self) -> &WindowBuffer {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use uuid::Uuid;
    #[tokio::test]
    async fn scratch_reader_simple() {
        let file = Uuid::new_v4().as_simple().to_string();
        let mut tmp_file = std::env::temp_dir();
        tmp_file.push(file);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_file)
            .await
            .unwrap();
        let mut string_control = String::new();
        for i in 0..1_000_000 {
            write!(&mut string_control, "{}\n", i).unwrap();
        }
        file.write_all(string_control.as_bytes()).await.unwrap();
        file.flush().await.unwrap();
        let file = tokio::fs::File::open(&tmp_file).await.unwrap();
        let mut scratch_reader = ScratchReader::new(file);
        let mut file_contents = Vec::new();
        scratch_reader
            .read_to_end(&mut file_contents)
            .await
            .unwrap();
        let scratch_contents = scratch_reader
            .buffer
            .get_bytes_all()
            .cloned()
            .collect::<Vec<u8>>();
        // contents of file are identical to the
        assert_eq!(file_contents, string_control.as_bytes());
        assert_eq!(string_control.as_bytes(), scratch_contents);
    }
}
