use std::{collections::VecDeque, mem, rc::Rc, str::from_utf8};

use anyhow::{bail, Context, Result};
use geo::{coord, Rect};
use proj::Proj;
use quick_xml::events::Event;
use rustc_hash::FxHashMap;

use crate::{
    index::{IndexedValue, Indexer},
    util::extend_rect::ExtendRect,
};

use super::srs_indexer::SRSContext;

/// The state the [`BoundingBoxIndexer`] is currently in
#[derive(Default)]
enum State {
    /// The indexer is in the initial state
    #[default]
    Initial,

    /// The indexer is currently parsing an XML element containing coordinates
    Parsing {
        /// The bounding box that the indexer was able to create so far from
        /// the parsed coordinates. `None` if not enough coordinates have been
        /// parsed yet.
        bbox: Option<Rect>,

        /// A queue of coordinates that were already parsed but could not be
        /// added to the bounding box yet
        remaining: VecDeque<f64>,
    },
}

/// Represents an intermediate bounding box created during parsing
struct IntermediateBoundingBox {
    /// The actual bounding box
    bbox: Rect,

    /// The SRS transformation object to convert the bounding box to WGS84
    proj: Rc<Proj>,
}

/// Calculates bounding boxes of CityGML geometries
#[derive(Default)]
pub struct BoundingBoxIndexer {
    /// The parser's current state
    state: State,

    /// A map of spatial reference systems and the bounding boxes that were
    /// created for each of them during the parsing process
    intermediate: FxHashMap<Rc<String>, IntermediateBoundingBox>,
}

impl BoundingBoxIndexer {
    /// Processes a queue of `remaining` coordinates and add them to the given
    /// bounding box. Takes as many coordinates out of the queue as the given
    /// dimension allows and leaves any coordinates that could not be added to
    /// the bounding box in the queue.
    fn process_remaining_coordinates(
        bbox: &mut Option<Rect>,
        remaining: &mut VecDeque<f64>,
        dim: u32,
    ) {
        while remaining.len() >= dim as usize {
            // take `dim` coordinates out of the queue
            let mut i = remaining.drain(0..dim as usize);

            // Try to get X, Y. Note that we ignore the Z value (if there is
            // any). The iterator will still drain `dim` elements from the
            // queue when it is dropped.
            let x = i.next().unwrap_or_default();
            let y = i.next().unwrap_or_default();

            if let Some(bbox) = bbox {
                bbox.extend_point(x, y);
            } else {
                bbox.replace(Rect::new(coord! { x: x, y: y }, coord! { x: x, y: y }));
            }
        }
    }

    /// Parses a given string of coordinates and adds them to the given bounding
    /// box. Fills the given queue with any remaining coordinates that could
    /// not be added.
    fn parse_coordinates(
        coords: &str,
        bbox: &mut Option<Rect>,
        remaining: &mut VecDeque<f64>,
        srs_context: &SRSContext,
    ) -> Result<()> {
        let numbers = coords.split_whitespace().filter(|p| !p.is_empty());
        for sn in numbers {
            let n = sn.parse::<f64>()?;
            remaining.push_back(n);
            if let Some(dim) = srs_context.current_srs_dimension() {
                Self::process_remaining_coordinates(bbox, remaining, dim);
            }
        }
        Ok(())
    }

    /// If the parser is in the state [`State::Parsing`], this function resets
    /// it to [`State::Initial`], processes any remaining coordinates, and
    /// adds them to the intermediate bounding boxes.
    fn finish_parsing_state(&mut self, srs_context: &SRSContext) -> Result<()> {
        if let State::Parsing {
            mut bbox,
            mut remaining,
        } = mem::replace(&mut self.state, State::Initial)
        {
            // Fail if we were able to parse coordinates but there are still
            // remaining ones. This can only mean that the number of coordinates
            // was not a multiple of the SRS dimension.
            if bbox.is_some() && !remaining.is_empty() {
                bail!(
                    concat!(
                        "The number of coordinates parsed was not a multiple ",
                        "of the specified SRS dimension {}. There are {} ",
                        "remaining coordinates.",
                    ),
                    srs_context.current_srs_dimension().unwrap(),
                    remaining.len()
                );
            }

            // We haven't created a bounding box yet but there are remaining
            // coordinates. This means the SRS dimension was not specified in
            // the XML document. Try to guess it and then process the coordinates.
            if bbox.is_none() && !remaining.is_empty() {
                let dim = srs_context
                    .current_srs_dimension()
                    .or(if remaining.len() % 3 == 0 {
                        Some(3)
                    } else if remaining.len() % 2 == 0 {
                        Some(2)
                    } else {
                        None
                    })
                    .context("Unknown SRS dimension. List of coordinates could not be parsed.")?;
                Self::process_remaining_coordinates(&mut bbox, &mut remaining, dim);
            }

            // add the bounding to the map of intermediate bounding boxes
            if let Some(bbox) = bbox {
                let srs = srs_context
                    .current_srs()
                    .context("Cannot parse bounding box. Unknown SRS.")?;

                // extend existing bounding box or add new entry
                let e = self.intermediate.get_mut(&srs);
                if let Some(intermediate_bbox) = e {
                    intermediate_bbox.bbox.extend_rect(&bbox);
                } else {
                    self.intermediate.insert(
                        Rc::clone(&srs),
                        IntermediateBoundingBox {
                            bbox,
                            proj: srs_context.get_srs_transformer(&srs)?,
                        },
                    );
                }
            }
        }

        Ok(())
    }
}

impl Indexer<(&Event<'_>, &SRSContext)> for BoundingBoxIndexer {
    fn on_event(&mut self, event: (&Event<'_>, &SRSContext)) -> Result<()> {
        let (event, srs_context) = event;
        match event {
            Event::Start(s) => {
                let local_name = s.local_name();
                match local_name.as_ref() {
                    b"lowerCorner" | b"upperCorner" | b"posList" | b"pos" => {
                        self.state = State::Parsing {
                            bbox: None,
                            remaining: VecDeque::new(),
                        };
                    }
                    _ => {}
                }
            }

            Event::End(e) => {
                let local_name = e.local_name();
                let lnr = local_name.as_ref();
                if lnr == b"lowerCorner"
                    || lnr == b"upperCorner"
                    || lnr == b"posList"
                    || lnr == b"pos"
                {
                    self.finish_parsing_state(srs_context)?;
                }
            }

            Event::Text(t) => {
                if let State::Parsing {
                    ref mut bbox,
                    ref mut remaining,
                } = self.state
                {
                    let s = t.unescape()?;
                    Self::parse_coordinates(&s, bbox, remaining, srs_context)?;
                }
            }

            Event::CData(d) => {
                if let State::Parsing {
                    ref mut bbox,
                    ref mut remaining,
                } = self.state
                {
                    let s = from_utf8(d)?;
                    Self::parse_coordinates(s, bbox, remaining, srs_context)?;
                }
            }

            _ => {}
        }

        Ok(())
    }
}

/// Convert the [`BoundingBoxIndexer`] to an [`IndexedValue`]
impl TryFrom<BoundingBoxIndexer> for Option<IndexedValue> {
    type Error = anyhow::Error;

    fn try_from(value: BoundingBoxIndexer) -> Result<Self> {
        let mut result: Option<Rect> = None;

        for (_, IntermediateBoundingBox { bbox, proj }) in value.intermediate {
            let mut points = [(bbox.min().x, bbox.min().y), (bbox.max().x, bbox.max().y)];
            proj.convert_array(&mut points)?;
            let converted = Rect::new(
                coord! { x: points[0].0, y: points[0].1 },
                coord! { x: points[1].0, y: points[1].1 },
            );
            if let Some(ref mut r) = result {
                r.extend_rect(&converted);
            } else {
                result = Some(converted);
            }
        }

        Ok(result.map(IndexedValue::BoundingBox))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use assertor::{assert_that, FloatAssertion, ResultAssertion};
    use geo::{coord, Rect};
    use proj::Proj;
    use quick_xml::{events::Event, Reader};

    use crate::index::{gml::srs_indexer::SRSIndexer, IndexedValue, Indexer};

    use super::BoundingBoxIndexer;

    fn parse(xml: &str) -> Result<Option<IndexedValue>> {
        let mut srs_indexer = SRSIndexer::default();
        let mut bbox_indexer = BoundingBoxIndexer::default();
        let mut reader = Reader::from_str(xml);
        let mut buf = Vec::new();
        loop {
            let e = reader.read_event_into(&mut buf)?;
            if !matches!(e, Event::End(_)) {
                srs_indexer.on_event(&e)?;
            }
            bbox_indexer.on_event((&e, srs_indexer.context()))?;
            if matches!(e, Event::End(_)) {
                srs_indexer.on_event(&e)?;
            }
            if e == Event::Eof {
                break;
            }
            buf.clear();
        }

        bbox_indexer.try_into()
    }

    fn assert_bbox(result: &IndexedValue, expected: Rect) {
        let proj = Proj::new_known_crs("EPSG:25832", "WGS84", None).unwrap();
        let mut arr = [
            (expected.min().x, expected.min().y),
            (expected.max().x, expected.max().y),
        ];
        proj.convert_array(&mut arr).unwrap();

        match result {
            IndexedValue::BoundingBox(bbox) => {
                assert_that!(bbox.min().x).is_approx_equal_to(arr[0].0);
                assert_that!(bbox.min().y).is_approx_equal_to(arr[0].1);
                assert_that!(bbox.max().x).is_approx_equal_to(arr[1].0);
                assert_that!(bbox.max().y).is_approx_equal_to(arr[1].1);
            }
            _ => panic!("Test failed: value is not a BoundingBox"),
        }
    }

    /// Tests if an empty object can be parsed
    #[test]
    fn empty() {
        let xml = r#"
            <LinearRing srsName="EPSG:25832" srsDimension="3" />
        "#;

        let value = parse(xml).unwrap();
        assert!(value.is_none());
    }

    /// Tests if we fail if the SRS has not been set
    #[test]
    fn no_srs() {
        let xml = r#"
            <LinearRing srsDimension="3">
                <posList>1 1 1 3 3 5</posList>
            </LinearRing>
        "#;

        let value = parse(xml);
        assert_that!(value).is_err();
    }

    /// Tests if we fail if the dimension has not been set and it cannot
    /// be guessed
    #[test]
    fn no_dimension_no_guess() {
        let xml = r#"
            <LinearRing srsName="EPSG:25832">
                <posList>1 1 1 3 3</posList>
            </LinearRing>
        "#;

        let value = parse(xml);
        assert_that!(value).is_err();
    }

    /// Tests if a simple object can be parsed to a bounding box
    #[test]
    fn simple_bbox() {
        let xml = r#"
            <LinearRing srsName="EPSG:25832" srsDimension="3">
                <posList>675603 6522325 0 675604 6522326 100</posList>
            </LinearRing>
        "#;

        let value = parse(xml).unwrap().unwrap();
        assert_bbox(
            &value,
            Rect::new(
                coord! { x: 675603.0, y: 6522325.0 },
                coord! { x: 675604.0, y: 6522326.0 },
            ),
        );
    }

    /// Tests if a simple object without a dimension can be parsed to a bounding box
    #[test]
    fn simple_bbox_with_dimension_guess() {
        let xml_2d = r#"
            <LinearRing srsName="EPSG:25832">
                <posList>675603 6522325 675604 6522326</posList>
            </LinearRing>
        "#;

        let value_2d = parse(xml_2d).unwrap().unwrap();
        assert_bbox(
            &value_2d,
            Rect::new(
                coord! { x: 675603.0, y: 6522325.0 },
                coord! { x: 675604.0, y: 6522326.0 },
            ),
        );

        let xml_3d = r#"
            <LinearRing srsName="EPSG:25832">
                <posList>675603 6522325 0 675604 6522326 100</posList>
            </LinearRing>
        "#;

        let value_3d = parse(xml_3d).unwrap().unwrap();
        assert_bbox(
            &value_3d,
            Rect::new(
                coord! { x: 675603.0, y: 6522325.0 },
                coord! { x: 675604.0, y: 6522326.0 },
            ),
        );
    }

    /// Tests if a two geometries can be parsed to a bounding box
    #[test]
    fn two_geometries() {
        let xml = r#"
            <MultiSurface srsName="EPSG:25832" srsDimension="3">
                <LinearRing>
                    <posList>675603 6522325 0 675604 6522326 100</posList>
                </LinearRing>
                <LinearRing>
                    <posList>675613 6522335 -10 675614 6522336 110</posList>
                </LinearRing>
            </MultiSurface>
        "#;

        let value = parse(xml).unwrap().unwrap();
        assert_bbox(
            &value,
            Rect::new(
                coord! { x: 675603.0, y: 6522325.0 },
                coord! { x: 675614.0, y: 6522336.0 },
            ),
        );
    }

    /// Tests if a two geometries with different dimensions can be parsed to a bounding box
    #[test]
    fn different_dimensions() {
        let xml = r#"
            <MultiSurface srsName="EPSG:25832" srsDimension="3">
                <LinearRing>
                    <posList>675603 6522325 0 675604 6522326 100</posList>
                </LinearRing>
                <LinearRing srsDimension="2">
                    <posList>675613 6522335 675614 6522336</posList>
                </LinearRing>
            </MultiSurface>
        "#;

        let value = parse(xml).unwrap().unwrap();
        assert_bbox(
            &value,
            Rect::new(
                coord! { x: 675603.0, y: 6522325.0 },
                coord! { x: 675614.0, y: 6522336.0 },
            ),
        );
    }

    /// Tests if a two geometries with different SRSs can be parsed to a bounding box
    #[test]
    fn different_srs() {
        let xml = r#"
            <MultiSurface srsDimension="3">
                <LinearRing srsName="EPSG:25832">
                    <posList>675603 6522325 0 675604 6522326 100</posList>
                </LinearRing>
                <LinearRing srsName="EPSG:31467">
                    <posList>3675748.2714459524 6524505.877373102 -10 3675749.2718322095 6524506.877780781 110</posList>
                </LinearRing>
            </MultiSurface>
        "#;

        let value = parse(xml).unwrap().unwrap();
        assert_bbox(
            &value,
            Rect::new(
                coord! { x: 675603.0, y: 6522325.0 },
                coord! { x: 675614.0, y: 6522336.0 },
            ),
        );
    }
}
