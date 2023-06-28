use actson::feeder::{DefaultJsonFeeder, JsonFeeder};
use actson::{JsonEvent, JsonParser};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader};
use tokio::runtime::Builder;

struct GeoJsonSplitter<R> {
    reader: BufReader<R>,
    feeder: DefaultJsonFeeder,
    parser: JsonParser,
}

struct IoFeeder {}

impl<R> GeoJsonSplitter<R>
where
    R: AsyncRead,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            parser: Default::default(),
            feeder: DefaultJsonFeeder::new(),
        }
    }

    pub async fn next_event(&mut self) -> std::io::Result<JsonEvent> {
        loop {
            match self.parser.next_event(&mut self.feeder) {
                JsonEvent::NeedMoreInput => {
                    let buffer = self.reader.fill_buf().await?;
                    if buffer.len() == 0 {
                        self.feeder.done();
                    } else {
                        // copy bytes into the feeder
                        let consumed = self.feeder.feed_bytes(buffer);
                        // consume them in the reader
                        self.reader.consume(consumed);
                    }
                }
                other => return Ok(other),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    // creates a path to the specified file in the temp directory
    fn tmp_file_path(file: Path) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(file);
        path
    }

    fn to_file(file: Path, contents: &str) {}

    #[tokio::test]
    async fn doesnt_panic() {
        let empty = "{}";
    }
}
