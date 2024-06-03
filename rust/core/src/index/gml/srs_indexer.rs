use std::{
    cell::RefCell,
    collections::hash_map::Entry::{Occupied, Vacant},
    rc::Rc,
};

use anyhow::{bail, Result};
use proj::Proj;
use quick_xml::events::{BytesStart, Event};
use rustc_hash::FxHashMap;

use crate::index::Indexer;

/// Provides information about the spatial reference system that applies to
/// the current XML element and its children
#[derive(Default)]
pub struct SRSContext {
    /// The current parsing depth
    current_parsing_depth: usize,

    /// A stack of spatial reference system names
    current_srs_name: Vec<(usize, Rc<String>)>,

    /// A stack of SRS dimensions
    current_srs_dimension: Vec<(usize, u32)>,

    /// A cache of SRS transformation objects from a given SRS to WGS84
    srs_transformer_cache: RefCell<FxHashMap<Rc<String>, Rc<Proj>>>,
}

impl SRSContext {
    /// Returns the spatial reference system that applies to the current XML
    /// element and its children
    pub fn current_srs(&self) -> Option<Rc<String>> {
        self.current_srs_name.last().map(|(_, s)| Rc::clone(s))
    }

    /// Returns the current spatial reference system dimension
    pub fn current_srs_dimension(&self) -> Option<u32> {
        self.current_srs_dimension.last().map(|d| d.1)
    }

    /// Returns an SRS transformation object for the given SRS to WGS84.
    pub fn get_srs_transformer(&self, srs_name: &Rc<String>) -> Result<Rc<Proj>> {
        let mut cache = self.srs_transformer_cache.borrow_mut();
        Ok(match cache.entry(Rc::clone(srs_name)) {
            Occupied(e) => Rc::clone(e.get()),
            Vacant(e) => {
                let p = Rc::new(Proj::new_known_crs(srs_name, "WGS84", None)?);
                Rc::clone(e.insert(p))
            }
        })
    }
}

#[derive(Default)]
pub struct SRSIndexer {
    context: SRSContext,
}

/// Examines each XML element and determines the spatial reference system that
/// applies to it
impl SRSIndexer {
    /// Returns the context that contains information about the parsed spatial
    /// reference system
    pub fn context(&self) -> &SRSContext {
        &self.context
    }

    /// Returns the context that contains information about the parsed spatial
    /// reference system
    pub fn context_mut(&mut self) -> &mut SRSContext {
        &mut self.context
    }
}

impl SRSIndexer {
    fn get_depth_from_start_element(&self, s: &BytesStart) -> usize {
        let local_name = s.local_name();
        if local_name.as_ref() == b"Envelope" && self.context.current_parsing_depth >= 2 {
            // The SRS of an Envelope applies to the whole
            // parent object. Envelopes are inside a <boundedBy>
            // element, so we need to go up 2 levels.
            self.context.current_parsing_depth - 2
        } else {
            self.context.current_parsing_depth
        }
    }
}

impl Indexer<&Event<'_>> for SRSIndexer {
    fn on_event(&mut self, event: &Event<'_>) -> Result<()> {
        match event {
            Event::Start(s) => {
                self.context.current_parsing_depth += 1;

                let srs_name_attr = s.try_get_attribute("srsName")?;
                if let Some(srs_name_attr) = srs_name_attr {
                    let srs_name = srs_name_attr.unescape_value()?;
                    if self.context.current_srs_name.is_empty()
                        || self.context.current_srs_name.last().unwrap().1.as_ref()
                            != srs_name.as_ref()
                    {
                        let d = self.get_depth_from_start_element(s);
                        self.context
                            .current_srs_name
                            .push((d, Rc::new(srs_name.to_string())));
                    }
                }

                let srs_dimension = s.try_get_attribute("srsDimension")?;
                if let Some(srs_dimension) = srs_dimension {
                    let dim = srs_dimension.unescape_value()?.parse::<u32>()?;
                    if dim < 2 {
                        bail!(
                            "Invalid `srsDimension' attribute: {}. Value must be at least 2.",
                            dim
                        );
                    }
                    if self.context.current_srs_dimension.is_empty()
                        || self.context.current_srs_dimension.last().unwrap().1 != dim
                    {
                        let d = self.get_depth_from_start_element(s);
                        self.context.current_srs_dimension.push((d, dim));
                    }
                }
            }

            Event::End(_) => {
                if let Some(current) = self.context.current_srs_name.last() {
                    if current.0 == self.context.current_parsing_depth {
                        self.context.current_srs_name.pop();
                    }
                }
                if let Some(current) = self.context.current_srs_dimension.last() {
                    if current.0 == self.context.current_parsing_depth {
                        self.context.current_srs_dimension.pop();
                    }
                }
                self.context.current_parsing_depth -= 1;
            }

            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{ptr, rc::Rc};

    use assertor::{assert_that, EqualityAssertion, OptionAssertion};
    use quick_xml::{events::Event, Reader};

    use crate::index::Indexer;

    use super::SRSIndexer;

    /// Parse an example CityGML file and check at various points if SRS and
    /// dimension are set correctly
    #[test]
    fn example_citygml_document() {
        let xml = r#"
            <!-- ASSERT 0 -->
            <CityModel>
                <!-- ASSERT 1 -->
                <boundedBy>
                    <Envelope srsName="EPSG:25832" srsDimension="3">
                        <lowerCorner>675603 6522325 0</lowerCorner>
                        <upperCorner>675604 6522326 100</upperCorner>
                    </Envelope>
                </boundedBy>
                <!-- ASSERT 2 -->
                <cityObjectMember>
                    <Building>
                        <boundedBy>
                            <GroundSurface>
                                <MultiSurface srsName="EPSG:2263" srsDimension="2">
                                    <surfaceMember>
                                        <!-- ASSERT 3 -->
                                    </surfaceMember>
                                </MultiSurface>
                            </GroundSurface>
                        </boundedBy>
                        <!-- ASSERT 4 -->
                        <boundedBy>
                            <RoofSurface>
                                <MultiSurface>
                                    <surfaceMember>
                                        <!-- ASSERT 5 -->
                                    </surfaceMember>
                                </MultiSurface>
                            </RoofSurface>
                        </boundedBy>
                    </Building>
                </cityObjectMember>
                <!-- ASSERT 6 -->
            </CityModel>
            <!-- ASSERT 7 -->
        "#;

        let mut srs_indexer = SRSIndexer::default();
        let mut reader = Reader::from_str(xml);
        let mut buf = Vec::new();
        loop {
            let e = reader.read_event_into(&mut buf).unwrap();
            srs_indexer.on_event(&e).unwrap();

            match e {
                Event::Comment(c) => {
                    let m = c.unescape().unwrap();
                    let ms = m.trim();
                    match ms {
                        "ASSERT 0" | "ASSERT 1" | "ASSERT 7" => {
                            // SRS has either not been set yet or it has been
                            // removed from the stack after the last end element
                            assert_that!(srs_indexer.context().current_srs()).is_none();
                            assert_that!(srs_indexer.context().current_srs_dimension()).is_none();
                        }
                        "ASSERT 2" | "ASSERT 4" | "ASSERT 5" | "ASSERT 6" => {
                            // The global SRS defined in the Envelope applies
                            assert_that!(srs_indexer.context().current_srs())
                                .is_equal_to(Some(Rc::new("EPSG:25832".to_string())));
                            assert_that!(srs_indexer.context().current_srs_dimension())
                                .is_equal_to(Some(3));
                        }
                        "ASSERT 3" => {
                            // A local SRS applies just to the MultiSurface
                            assert_that!(srs_indexer.context().current_srs())
                                .is_equal_to(Some(Rc::new("EPSG:2263".to_string())));
                            assert_that!(srs_indexer.context().current_srs_dimension())
                                .is_equal_to(Some(2));
                        }
                        _ => panic!("Unknown comment: {}", ms),
                    }
                }
                Event::Eof => break,
                _ => {}
            }

            buf.clear();
        }
    }

    /// Test if we can get a SRS transformation object from the context and
    /// if the context caches it
    #[test]
    fn transformer_cache() {
        let mut srs_indexer = SRSIndexer::default();
        let c = srs_indexer.context_mut();
        let t1 = c
            .get_srs_transformer(&Rc::new("EPSG:25832".to_string()))
            .unwrap();
        let t2 = c
            .get_srs_transformer(&Rc::new("EPSG:25832".to_string()))
            .unwrap();
        assert!(ptr::eq(t1.as_ref(), t2.as_ref()));

        let srs2 = Rc::new("EPSG:25834".to_string());
        let t3 = c.get_srs_transformer(&srs2).unwrap();
        let t4 = c.get_srs_transformer(&srs2).unwrap();
        assert!(ptr::eq(t3.as_ref(), t4.as_ref()));

        // SRS 1 should still be there
        let t5 = c
            .get_srs_transformer(&Rc::new("EPSG:25832".to_string()))
            .unwrap();
        assert!(ptr::eq(t1.as_ref(), t5.as_ref()));
    }
}
