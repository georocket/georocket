use actson::feeder::PushJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::{bail, Result};
use async_channel;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc;

type Chunk = Vec<(JsonEvent, Payload)>;
const EXTEND: usize = 1024;

mod buffer;
use buffer::Buffer;

struct GeoJsonSplitter<R> {
    reader: BufReader<R>,
    parser: JsonParser<PushJsonFeeder>,
    buffer: Buffer,
    chunk_out: async_channel::Sender<Chunk>,
    raw_out: tokio::sync::mpsc::Sender<String>,
}

#[derive(Debug, Clone)]
enum Payload {
    String(String),
    Int(i64),
    Double(f64),
    None,
}

#[derive(PartialEq, Debug)]
enum GeoJsonType {
    Object,
    Collection,
}

// specifies all the field names that are of interest
enum FieldNames {
    // might be useful, to determine what type of GeoJSON object we are in
    Type,
    // marks start of an array of GeoJSON features
    Features,
    // marks start of an array of GeoJSON geometries
    Geometries,
    Unknown,
}

impl From<&str> for FieldNames {
    fn from(value: &str) -> Self {
        match value {
            "type" => FieldNames::Type,
            "geometries" => FieldNames::Geometries,
            "features" => FieldNames::Features,
            _ => FieldNames::Unknown,
        }
    }
}

impl<R> GeoJsonSplitter<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(
        reader: R,
        chunk_out: async_channel::Sender<Chunk>,
        raw_out: mpsc::Sender<String>,
    ) -> Self {
        Self {
            reader: BufReader::new(reader),
            parser: JsonParser::new(PushJsonFeeder::new()),
            buffer: Buffer::new(),
            chunk_out,
            raw_out,
        }
    }

    async fn run(mut self) -> Result<GeoJsonType> {
        self.process_top_level_object().await
    }

    async fn process_top_level_object(&mut self) -> Result<GeoJsonType> {
        use JsonEvent::*;
        self.find_next(&[StartObject]).await?;
        let begin = self.parser.parsed_bytes();
        self.buffer.set_marker(begin - 1);
        let mut chunk: Chunk = vec![(StartObject, Payload::None)];
        let mut depth: u32 = 1;
        let mut geo_json_type = GeoJsonType::Object;
        loop {
            let event = self.parser.next_event();
            let payload = match event {
                Error => bail!("the jason parser has encountered an error"),
                Eof => bail!("unexpected EOF while parsing object"),
                NeedMoreInput => {
                    self.fill_feeder().await?;
                    continue;
                }
                StartObject => {
                    depth += 1;
                    Payload::None
                }
                EndObject => {
                    depth -= 1;
                    Payload::None
                }
                FieldName | ValueString | ValueInt | ValueDouble => {
                    Self::extract_payload(event, self.parser.current_string()?)?
                }
                _ => Payload::None,
            };
            if event == FieldName {
                if let Payload::String(f) = &payload {
                    match f.as_str().into() {
                        FieldNames::Features | FieldNames::Geometries => {
                            self.process_collection().await?;
                            geo_json_type = GeoJsonType::Collection;
                            break;
                        }
                        _ => (),
                    }
                } else {
                    unreachable!("payload must be a string, otherwise it cannot be a field name")
                }
            }
            chunk.push((event, payload));
            if depth == 0 {
                break;
            }
        }
        match geo_json_type {
            GeoJsonType::Object => {
                let end = self.parser.parsed_bytes();
                let bytes = end - begin;
                let raw = String::from_utf8(self.buffer.retrieve_marked(bytes + 1))?;
                self.raw_out.send(raw).await?;
                self.chunk_out.send(chunk).await?;
                GeoJsonType::Object
            }
            GeoJsonType::Collection => GeoJsonType::Collection,
        };
        Ok(geo_json_type)
    }

    fn extract_payload(event: JsonEvent, payload: &str) -> anyhow::Result<Payload> {
        use JsonEvent::*;
        debug_assert!(
            (event == ValueDouble
                || event == ValueInt
                || event == ValueString
                || event == FieldName),
            "cannot extract payload from a field that is not ValueInt, ValueDouble or ValueString"
        );
        Ok(match event {
            ValueString | FieldName => Payload::String(payload.into()),
            ValueDouble => Payload::Double(payload.parse()?),
            ValueInt => Payload::Int(payload.parse()?),
            _ => unreachable!(),
        })
    }

    async fn fill_feeder(&mut self) -> anyhow::Result<()> {
        if self.buffer.end() {
            // if all bytes from the buffer have been consumed, then fill the buffer
            // from the reader
            let read_bytes = self.reader.fill_buf().await?;
            let byte_count = usize::min(EXTEND, read_bytes.len());
            self.buffer.fill_bytes(&read_bytes[..byte_count]);
            self.reader.consume(byte_count);
        }
        let (front, back) = self.buffer.get_bytes(EXTEND);
        let from_front = self.parser.feeder.push_bytes(front);
        let from_back = self.parser.feeder.push_bytes(back);
        self.buffer.consume(from_front + from_back);
        Ok(())
    }

    async fn find_next(&mut self, events: &[JsonEvent]) -> Result<JsonEvent> {
        loop {
            let e = self.parser.next_event();
            match e {
                event if events.contains(&event) => return Ok(event),
                JsonEvent::NeedMoreInput => self.fill_feeder().await?,
                JsonEvent::Eof => {
                    bail!("unexpected EOF encountered while searching for one of: {events:?}")
                }
                JsonEvent::Error => bail!("the parser has encountered an error"),
                _ => continue,
            }
        }
    }

    async fn process_collection(&mut self) -> Result<()> {
        self.find_next(&[JsonEvent::StartArray]).await?;
        // tells us how many bytes have previously been parsed, before a drain happened.
        // We can then use this to determine the offset from the start of the buffer to the
        // start of the object of interest
        let mut previously_parsed = 0;
        // 1. process the next chunk
        // 2. retrieve the bytes from the buffer
        //  how do we know how many bytes?
        //    => call `parsed_bytes()` before and after getting a chunk
        //  how does `next_chunk()` know where to set the
        // 3. drain the buffer
        loop {
            match self
                .find_next(&[JsonEvent::EndArray, JsonEvent::StartObject])
                .await?
            {
                JsonEvent::EndArray => break,
                JsonEvent::StartObject => {
                    let begin = self.parser.parsed_bytes();
                    let buffer_offset = begin - previously_parsed;
                    // buffer_offset - 1, because 0 indexing
                    self.buffer.set_marker(buffer_offset - 1);
                    let chunk = self.next_chunk().await?;
                    let end = self.parser.parsed_bytes();
                    let count = end - begin;

                    // account for subtracting 1 above
                    let bytes = self.buffer.retrieve_marked(count + 1);
                    self.buffer.drain_marked(count + 1);

                    let raw = String::from_utf8(bytes)?;

                    // send out the data
                    self.chunk_out.send(chunk).await?;
                    self.raw_out.send(raw).await?;
                    previously_parsed = end;
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    async fn next_chunk(&mut self) -> anyhow::Result<Chunk> {
        use JsonEvent::*;
        let mut chunk: Chunk = vec![(StartObject, Payload::None)];
        let mut depth: u32 = 1;
        // this function gets called, when the parser has found the start of an object,
        // so self.parser.parsed_bytes is the start of the object
        loop {
            let event = self.parser.next_event();
            let payload = match event {
                Error => bail!("the json parser has encountered an error"),
                Eof => bail!("unexpected EOF while parsing object"),
                NeedMoreInput => {
                    self.fill_feeder().await?;
                    continue;
                }
                StartObject => {
                    depth += 1;
                    Payload::None
                }
                EndObject => {
                    depth -= 1;
                    Payload::None
                }
                FieldName | ValueString | ValueInt | ValueDouble => {
                    Self::extract_payload(event, self.parser.current_string()?)?
                }
                _ => Payload::None,
            };
            chunk.push((event, payload));
            if depth == 0 {
                break;
            }
        }
        Ok(chunk)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{path::Path, slice::Chunks};
    use tokio::fs::{read_to_string, File};

    /// Splits the geo_json into features or geometries, returns handles to the task the splitter
    /// is running on, as well as handles for the tasks that collect the results the splitter is
    /// sending out through it's channels.
    async fn split_geo_json(
        path: &Path,
    ) -> (
        tokio::task::JoinHandle<Result<GeoJsonType, anyhow::Error>>,
        tokio::task::JoinHandle<Vec<String>>,
        tokio::task::JoinHandle<Vec<Chunk>>,
    ) {
        let geo_json = File::open(path).await.unwrap();
        let (chunk_send, chunk_rec) = async_channel::unbounded();
        let (raw_send, mut raw_rec) = mpsc::channel(1024);
        let splitter = GeoJsonSplitter::new(geo_json, chunk_send, raw_send);
        let splitter_handle = tokio::spawn(splitter.run());
        let raw_string_hanlde = tokio::spawn(async move {
            let mut raw_strings = Vec::new();
            while let Some(feature) = raw_rec.recv().await {
                raw_strings.push(feature);
            }
            raw_strings
        });
        let chuncks_handle = tokio::spawn(async move {
            let mut chunks = Vec::new();
            while let Ok(chunk) = chunk_rec.recv().await {
                chunks.push(chunk);
            }
            chunks
        });
        (splitter_handle, raw_string_hanlde, chuncks_handle)
    }

    #[tokio::test]
    async fn simple_feature() {
        let control = tokio::spawn(read_to_string("test_files/simple_feature_01.json"));
        let (splitter, raw_strings, chunks) =
            split_geo_json(Path::new("test_files/simple_feature_01.json")).await;
        let _ = File::open("test_files/simple_feature_01.json")
            .await
            .unwrap();
        let chunk = chunks.await.unwrap()[0].clone();
        let raw = raw_strings.await.unwrap()[0].clone();
        let control = control.await.unwrap().unwrap();
        let geo_json_type = splitter.await.unwrap().unwrap();
        assert_eq!(raw, control);
        assert_eq!(geo_json_type, GeoJsonType::Object);
    }

    #[tokio::test]
    async fn simple_collection() {
        let c1 = r#"{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [ 102.0, 0.5 ] }, "properties": { "prop0": "value0" } }"#; //.to_string();
        let c2 = r#"{ "type": "Feature", "geometry": { "type": "LineString", "coordinates": [ [ 102.0, 0.0 ], [ 103.0, 1.0 ], [ 104.0, 0.0 ], [ 105.0, 1.0 ] ] }, "properties": { "prop0": "value0", "prop1": 0.0 } }"#; //.to_string();
        let c3 = r#"{ "type": "Feature", "geometry": { "type": "Polygon", "coordinates": [ [ [ 100.0, 0.0 ], [ 101.0, 0.0 ], [ 101.0, 1.0 ], [ 100.0, 1.0 ], [ 100.0, 0.0 ] ] ] }, "properties": { "prop0": "value0", "prop1": { "this": "that" } } }"#; //.to_string();
        let mut control_strings = vec![c1, c2, c3];
        let (splitter, raw_strings, chunks) =
            split_geo_json(Path::new("test_files/simple_collection_01.json")).await;
        let raw_strings = raw_strings.await.unwrap();
        let chunks = chunks.await.unwrap();
        for feature in raw_strings {
            let index = control_strings.iter().position(|f| f == &feature).unwrap();
            assert_eq!(control_strings[index], feature);
            control_strings.remove(index);
        }
        // all control strings should have been removed from the control string collection
        assert!(control_strings.len() == 0);
    }
}
