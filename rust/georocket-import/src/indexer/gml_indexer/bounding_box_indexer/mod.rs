use anyhow::{anyhow, Context};
use indexing::bounding_box::{BoundingBoxBuilder, NoValidation};
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use std::num::ParseIntError;
use std::str::FromStr;

mod coordinate_iter;
use coordinate_iter::CoordinateIterator;

mod epsg_code_extraction;
use epsg_code_extraction::get_epsg_code;
use georocket_types::BoundingBox;

#[derive(Copy, Clone, Eq, PartialEq)]
enum Dimensions {
    Unknown,
    D2,
    D3,
}

impl Dimensions {
    fn new(dim: u16) -> Self {
        match dim {
            2 => Self::D2,
            3 => Self::D3,
            _ => Self::Unknown,
        }
    }
}

impl FromStr for Dimensions {
    type Err = ParseIntError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self::new(s.parse()?))
    }
}

fn get_dimensions(payload: Option<&Attribute>) -> anyhow::Result<Dimensions> {
    if let Some(dim) = payload.map(|p| p.value.as_ref()) {
        let dim = std::str::from_utf8(dim).context("dimension attribute contained invalid utf8")?;
        Ok(dim
            .parse::<Dimensions>()
            .context("failed to parse provided attribute value into Dimension")?)
    } else {
        Ok(Dimensions::Unknown)
    }
}

enum State {
    Uninitialized,
    Processing,
    ParseCorner,
    ParsePosList,
}

struct BoundingBoxIndexer {
    dimensions: Dimensions,
    bounding_box_builder: BoundingBoxBuilder<NoValidation>,
    state: State,
    srs: Option<u16>,
}

impl BoundingBoxIndexer {
    fn new() -> Self {
        Self {
            dimensions: Dimensions::Unknown,
            state: State::Uninitialized,
            srs: None,
            bounding_box_builder: BoundingBoxBuilder::new(NoValidation),
        }
    }
    fn retrieve_bounding_box(self) -> Option<BoundingBox> {
        // # Panic Safety:
        // `build` method is Infallible with BoundingBoxBuilder<NoValidation>
        self.bounding_box_builder.build().expect("")
    }
    fn process_event(&mut self, event: &Event) -> anyhow::Result<()> {
        match event {
            Event::Start(payload) => self.handle_start_event(payload),
            Event::Text(payload) => self.handle_text(payload),
            Event::End(payload) => self.handle_end(payload),
            _ => Ok(()),
        }
    }
    fn handle_start_event<'a>(&mut self, payload: &'a BytesStart) -> anyhow::Result<()> {
        let retrieve_attribute = |name| {
            payload
                .try_get_attribute(name)
                .context("malformed attributes in xml-chunk")
        };
        retrieve_attribute("srsName")?
            .map(|srs_name| self.handle_srs(&srs_name))
            .transpose()?;
        payload
            .try_get_attribute("srsName")
            .context("malformed attributes in xml-chunk")?
            .map(|srs_name| self.handle_srs(&srs_name))
            .transpose()?;
        match self.state {
            State::Uninitialized => {}
            State::Processing => match payload.local_name().as_ref() {
                b"Envelope" => {
                    let srs_dimensions = retrieve_attribute("srsDimensions")?;
                    self.dimensions = get_dimensions(srs_dimensions.as_ref())?;
                }
                b"pos" | b"posList" => {
                    let srs_dimensions = retrieve_attribute("srsDimensions")?;
                    self.dimensions = get_dimensions(srs_dimensions.as_ref())?;
                    self.state = State::ParsePosList
                }
                b"lowerCorner" | b"upperCorner" => self.state = State::ParseCorner,
                _ => {}
            },
            _ => {}
        }
        Ok(())
    }
    fn handle_srs(&mut self, srs_name: &Attribute) -> anyhow::Result<()> {
        // TODO: Handle multiple srs in one chunk
        // Blockers: Transforming between srs systems (proj)
        let srs_name = std::str::from_utf8(&srs_name.value)?;
        let epsg_code = get_epsg_code(srs_name)?;
        if self.srs.is_some_and(|srs| srs != epsg_code) {
            Err(anyhow!(
                "multiple spatial reference systems within a single chunk not supported"
            ))
        } else {
            self.srs = Some(epsg_code);
            self.bounding_box_builder = self.bounding_box_builder.set_srid(epsg_code as u32);
            self.state = State::Processing;
            Ok(())
        }
    }
    fn handle_text(&mut self, payload: &BytesText) -> anyhow::Result<()> {
        match self.state {
            State::Uninitialized => Ok(()),
            State::Processing => Ok(()),
            State::ParseCorner | State::ParsePosList => {
                let coordinates = payload
                    .unescape()
                    .context("unable to convert payload to utf8")?;
                let coordinates = CoordinateIterator::new(coordinates.as_ref(), self.dimensions);
                for coord in coordinates {
                    let (x, y) = coord?;
                    self.bounding_box_builder.add_point_mut(x, y);
                }
                Ok(())
            }
        }
    }
    fn handle_end(&mut self, payload: &BytesEnd) -> anyhow::Result<()> {
        match payload.local_name().as_ref() {
            b"Envelope" => {
                self.dimensions = Dimensions::Unknown;
                self.state = State::Processing;
            }
            b"pos" | b"posList" => {
                self.dimensions = Dimensions::Unknown;
                self.state = State::Processing;
            }
            b"lowerCorner" | b"upperCorner" => {
                self.state = State::Processing;
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splitter::xml_splitter::FirstLevelSplitter;
    use crate::splitter::SplitterChannels;
    use crate::types::{InnerChunk, XMLChunk};
    use georocket_types::GeoPoint;
    use std::path::Path;

    #[tokio::test]
    async fn pos_list() {
        let chunks = get_chunks("test_files/gml/simple_envelope.xml").await;
        let chunk = &chunks[0];
        let mut bbox_indexer = BoundingBoxIndexer::new();
        for event in chunk {
            bbox_indexer.process_event(event).unwrap()
        }
        let bbox = bbox_indexer.retrieve_bounding_box().unwrap();
        let control = BoundingBox {
            srid: Some(3068),
            lower_left: GeoPoint::new(100., 100.),
            upper_right: GeoPoint::new(300., 300.),
        };
        assert_eq!(bbox, control)
    }

    async fn get_chunks(gml_file: impl AsRef<Path>) -> Vec<XMLChunk> {
        let (splitter_channels, chunk_receiver, _raw) = SplitterChannels::new_with_channels(10, 10);
        let feature = tokio::fs::File::open(gml_file).await.unwrap();
        let splitter = FirstLevelSplitter::new(feature, splitter_channels);
        let handle = tokio::spawn(async move { splitter.run().await });
        let mut chunks = Vec::new();
        while let Ok(chunk) = chunk_receiver.recv().await {
            let InnerChunk::XML(chunk) = chunk.inner else {
                panic!("chunk should be xml chunks")
            };
            chunks.push(chunk)
        }
        chunks
    }
}
