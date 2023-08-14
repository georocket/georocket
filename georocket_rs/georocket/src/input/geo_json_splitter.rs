use actson::feeder::PushJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::{bail, Result};
use async_channel;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::mpsc;

pub type GeoJsonChunk = Vec<(JsonEvent, Payload)>;

const EXTEND: usize = 1024;

mod buffer;
use buffer::Buffer;

use super::Chunk;

#[derive(Debug, Clone)]
pub enum Payload {
    String(String),
    Int(i64),
    Double(f64),
    None,
}

#[derive(PartialEq, Debug)]
pub enum GeoJsonType {
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

/// The `GeoJsonSplitter` takes GeoJSON from an asynchronous input and attemtps to split it into its
/// constituent parts. If the GeoJSON is a `FeatureCollection` or a `GeometryCollection`, it will
/// output the individual geometries or features through the the `raw_send` and `chunk_send` channels, to
/// be processed.
pub struct GeoJsonSplitter<R> {
    reader: BufReader<R>,
    parser: JsonParser<PushJsonFeeder>,
    buffer: Buffer,
    chunk_send: async_channel::Sender<Chunk>,
    raw_send: tokio::sync::mpsc::Sender<String>,
}

impl<R> GeoJsonSplitter<R>
where
    R: AsyncRead + Unpin,
{
    /// Constructs a new `GeoJsonSplitter` object that will read a GeoJSON file from the reader
    /// and send out the resulting chunks and features through the provided channels.  
    pub fn new(
        reader: R,
        chunk_send: async_channel::Sender<Chunk>,
        raw_send: mpsc::Sender<String>,
    ) -> Self {
        Self {
            reader: BufReader::new(reader),
            parser: JsonParser::new(PushJsonFeeder::new()),
            buffer: Buffer::new(),
            chunk_send,
            raw_send,
        }
    }

    /// Consumes the `GeoJsonSplitter` and initiates the processing of the associated GeoJSON file.
    pub async fn run(mut self) -> Result<GeoJsonType> {
        self.process_top_level_object().await
    }

    /// Begins the processing of the associated GeoJSON file, by assuming it to be a non-nested
    /// GeoJSON type (`Feature` or `Geometry`). If a field with the name "features" or "geometries"
    /// is encountered, it will switch to the `process_collection` method.
    ///
    /// Sends the resulting `Chunk` and the raw string of the feature through the respective
    /// channels for further processing
    async fn process_top_level_object(&mut self) -> Result<GeoJsonType> {
        use JsonEvent::*;
        self.find_next(&[StartObject]).await?;
        let begin = self.parser.parsed_bytes();
        self.buffer.set_marker(begin - 1);
        let mut chunk: GeoJsonChunk = vec![(StartObject, Payload::None)];
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
            // Check if the field name indicates that this is a `FeatureCollection` or
            // `GeometryCollection` and proceed accordingly, calling the `process_collection()`
            // method in such a case.
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
            // break out of the loop, if the GeoJSON object has been fully parsed.
            if depth == 0 {
                break;
            }
        }
        // check what type of GeoJSON object has been processed. If it's a non-nested feature
        // or geometry, it must still be extracted from the buffer and sent out.
        match geo_json_type {
            GeoJsonType::Object => {
                let end = self.parser.parsed_bytes();
                let count = (end - begin) + 1;
                let raw = String::from_utf8(self.buffer.retrieve_marked(count))?;
                self.raw_send.send(raw).await?;
                self.chunk_send.send(chunk.into()).await?;
                GeoJsonType::Object
            }
            GeoJsonType::Collection => GeoJsonType::Collection,
        };
        Ok(geo_json_type)
    }

    /// Attempts to extract the appropriate payload from the provided string.
    ///
    /// # Errors
    ///
    /// This function will error if the string cannot be parsed into the appropriate type.
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

    /// Fills the parsers feeder with bytes from the buffer.
    /// Fills the buffer from the reader, if all bytes in the buffer have been consumed.
    ///
    /// # Errors
    ///
    /// This method will error, if all the bytes in the buffer have been consumed and attempting
    /// to retrieve more from the reader fails.
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

    /// Attempts to advances the parser to the next `JsonEvent`, that matches one of the `JsonEvent`s specified
    /// in `events`.
    ///
    /// # Errors
    ///
    /// This method well return an error, if a JsonEvent::Eof or JsonEvent::Error is encountered
    /// and these are not specified in `events`.
    /// It will also return an error, if a call to `fill_feeder` fails, if the parser requests more
    /// bytes.
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

    /// Attempts to process an array of `Feature` or `Geometry` GeoJSON objects.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    ///    + It cannot find the start of the array that is supposed to conatin the objects
    ///    + An objects or the array is malformed
    ///    + Parsing retrieved bytes into a valid UTF8 string fails
    ///    + Loading additional bytes from the reader fails
    ///    + Sending out processed `Chunk`s or the raw strings fails.
    async fn process_collection(&mut self) -> Result<()> {
        self.find_next(&[JsonEvent::StartArray]).await?;
        // tells us how many bytes have previously been parsed, before a drain happened.
        // We can then use this to determine the offset from the start of the buffer to the
        // start of the object of interest
        let mut previously_parsed = 0;
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
                    // +1 to account for subtracting 1 above
                    let count = (end - begin) + 1;
                    let bytes = self.buffer.retrieve_marked(count);
                    self.buffer.drain_marked(count);

                    let raw = String::from_utf8(bytes)?;

                    // send out the data
                    self.chunk_send.send(chunk.into()).await?;
                    self.raw_send.send(raw).await?;
                    previously_parsed = end;
                }
                _ => unreachable!("`find_next()` should have returned an error, if any other events are found here"),
            }
        }
        Ok(())
    }

    /// Attempts to process the next chunk in a collection.
    async fn next_chunk(&mut self) -> anyhow::Result<GeoJsonChunk> {
        use JsonEvent::*;
        let mut chunk: GeoJsonChunk = vec![(StartObject, Payload::None)];
        let mut depth: u32 = 1;
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
    use std::{collections::HashMap, path::Path};
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
        let (splitter_handle, chunk_rec, mut raw_rec) = setup(path).await;
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

    async fn setup(
        path: &Path,
    ) -> (
        tokio::task::JoinHandle<Result<GeoJsonType, anyhow::Error>>,
        async_channel::Receiver<Chunk>,
        tokio::sync::mpsc::Receiver<String>,
    ) {
        let geo_json = File::open(path).await.unwrap();
        let (chunk_send, chunk_rec) = async_channel::unbounded();
        let (raw_send, raw_rec) = mpsc::channel(1024);
        let splitter = GeoJsonSplitter::new(geo_json, chunk_send, raw_send);
        let splitter_handle = tokio::spawn(splitter.run());
        (splitter_handle, chunk_rec, raw_rec)
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
        assert!(control_strings.is_empty());
    }

    /// parse the given file with serde_json and return the inner Json `features` or `geometries`
    /// array as a Rust Vector
    fn parse_with_serde(path: &Path) -> Vec<serde_json::Value> {
        let file = std::fs::File::open(path).unwrap();
        let reader = std::io::BufReader::new(file);
        let mut contents: serde_json::Value = serde_json::from_reader(reader).unwrap();
        if let Some(features) = contents.get_mut("features") {
            if let serde_json::Value::Array(features) = std::mem::take(features) {
                features
            } else {
                panic!("field `features` did not contain an array")
            }
        } else if let Some(geometries) = contents.get_mut("geometries") {
            if let serde_json::Value::Array(geometries) = std::mem::take(geometries) {
                geometries
            } else {
                panic!("field `features` did not contain an array")
            }
        } else {
            vec![contents]
        }
    }

    #[tokio::test]
    async fn large_collection() {
        let file = Path::new("test_files/large_collection_01.json");
        let control_values = parse_with_serde(file);
        let mut control_values_map: std::collections::HashMap<String, usize> = HashMap::new();
        // convert values to strings and store the count of every unique string in `control_values_map`.
        control_values
            .iter()
            .map(|value| serde_json::to_string(&value).unwrap())
            .for_each(|value| {
                control_values_map
                    .entry(value)
                    .and_modify(|val| *val += 1)
                    .or_insert(1);
            });
        assert_eq!(
            control_values.len(),
            control_values_map.values().sum::<usize>()
        );
        let (splitter, chunks, mut raw_strings) = setup(file).await;
        while let Some(feature) = raw_strings.recv().await {
            // convert the feature to a serde_json::Value and back to a string. This insures the
            // formatting is the same
            let feature: serde_json::Value = serde_json::from_str(feature.as_str()).unwrap();
            let feature = serde_json::to_string(&feature).unwrap();
            control_values_map
                .entry(feature)
                .and_modify(|val| *val -= 1);
        }
        assert!(control_values_map.values().all(|val| *val == 0));
    }
}
