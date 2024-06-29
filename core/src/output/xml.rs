use anyhow::Result;
use itertools::Itertools;
use quick_xml::{
    escape::escape,
    events::{BytesStart, Event},
    name::QName,
    Reader,
};
use std::{borrow::Cow, io::Write};
use thiserror::Error;

use crate::{
    index::{
        chunk_meta::ChunkMeta,
        gml::root_element::{Prefix, RootElement},
    },
    output::Merger,
};

/// Errors that can occur during XML merging
#[derive(Error, Debug)]
pub enum XmlMergerError {
    #[error("multiple root elements encountered (found `{found:?}', expected `{expected:?}'")]
    MultipleRootElements { found: String, expected: String },

    #[error(
        "root element to merge is from unexpected namespace (found `{found:?}', expected `{expected:?}'"
    )]
    UnexpectedRootElementNamespace { found: String, expected: String },

    #[error("no namespace found for prefix `{0}'")]
    UnknownNamespacePrefix(Prefix),

    #[error("reached end of chunk without finding a start tag")]
    PrematureEndOfChunk,

    #[error("expected end of chunk but found additional content")]
    UnexpectedContent,

    #[error("unable to parse chunk")]
    Parser(#[from] quick_xml::Error),

    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

/// A merger that merges XML chunks
pub struct XmlMerger<W>
where
    W: Write,
{
    writer: W,
    root_element: Option<RootElement>,
}

impl<W> XmlMerger<W>
where
    W: Write,
{
    /// Create a new XML merger that writes into the given writer
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            root_element: None,
        }
    }
}

impl<W> Merger<XmlMergerError> for XmlMerger<W>
where
    W: Write,
{
    fn merge(&mut self, chunk: &[u8], meta: ChunkMeta) -> Result<(), XmlMergerError> {
        let new_chunk = if let Some(ref sre) = self.root_element {
            if *sre != meta.root_element {
                if sre.name == meta.root_element.name {
                    if prefix_namespace_eq(
                        &sre.prefix,
                        &sre.namespaces,
                        &meta.root_element.prefix,
                        &meta.root_element.namespaces,
                    )? {
                        let (new_start_element, remainder) =
                            merge_root_element_into_chunk(chunk, &meta.root_element, sre)?;
                        self.writer.write_all(&[b'<'])?;
                        self.writer.write_all(&new_start_element)?;
                        self.writer.write_all(&[b'>'])?;
                        remainder
                    } else {
                        let found = get_namespace(
                            &meta.root_element.prefix,
                            &meta.root_element.namespaces,
                        )?;
                        let expected = get_namespace(&sre.prefix, &sre.namespaces)?;
                        return Err(XmlMergerError::UnexpectedRootElementNamespace {
                            found: found.clone(),
                            expected: expected.clone(),
                        });
                    }
                } else {
                    return Err(XmlMergerError::MultipleRootElements {
                        found: meta.root_element.name,
                        expected: sre.name.clone(),
                    });
                }
            } else {
                Cow::from(chunk)
            }
        } else {
            self.writer.write_all(&[b'<'])?;
            self.writer.write_all(&meta.root_element.to_bytes_start())?;
            self.writer.write_all(&[b'>'])?;
            self.root_element = Some(meta.root_element);
            Cow::from(chunk)
        };

        self.writer.write_all(&new_chunk)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(re) = self.root_element.as_ref() {
            self.writer.write_all(b"</")?;
            self.writer.write_all(&re.to_bytes_end())?;
            self.writer.write_all(&[b'>'])?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

/// Get the namespace for a `prefix` from a given list of `namespaces`
fn get_namespace<'a>(
    prefix: &Prefix,
    namespaces: &'a [(Prefix, String)],
) -> Result<&'a String, XmlMergerError> {
    namespaces
        .iter()
        .find(|(p, _)| p == prefix)
        .map(|(_, n)| n)
        .ok_or_else(|| XmlMergerError::UnknownNamespacePrefix(prefix.clone()))
}

/// Check if the namespaces of two prefixes are equal
fn prefix_namespace_eq(
    prefix1: &Prefix,
    namespaces1: &[(Prefix, String)],
    prefix2: &Prefix,
    namespaces2: &[(Prefix, String)],
) -> Result<bool, XmlMergerError> {
    let ns1 = get_namespace(prefix1, namespaces1)?;
    let ns2 = get_namespace(prefix2, namespaces2)?;
    Ok(ns1 == ns2)
}

/// Split a chunk into start tag and remainder
fn split_chunk<'a>(
    chunk: &'a [u8],
    reader: &mut Reader<&'a [u8]>,
) -> Result<(BytesStart<'a>, Cow<'a, [u8]>), XmlMergerError> {
    loop {
        let e = reader.read_event()?;
        match e {
            Event::Start(bs) => return Ok((bs, Cow::from(&chunk[reader.buffer_position()..]))),

            Event::Empty(bs) => {
                let is_remainder_blank = reader
                    .decoder()
                    .decode(&chunk[reader.buffer_position()..])?
                    .trim()
                    .is_empty();
                if !is_remainder_blank {
                    return Err(XmlMergerError::UnexpectedContent);
                }

                // There is no start tag. The chunk consists of an empty tag
                // only. Generate a corresponding end tag and make this the
                // remainder.
                let mut end = vec![b'<', b'/'];
                end.extend(bs.to_owned().to_end().iter());
                end.push(b'>');
                return Ok((bs, Cow::from(end)));
            }

            Event::Eof => return Err(XmlMergerError::PrematureEndOfChunk),

            _ => {}
        }
    }
}

/// Merge namespaces into start tag if they don't exist yet. Also consider
/// overwrites and namespaces of the new parent.
fn merge_namespaces_into_start_tag(
    bytes_start: &mut BytesStart,
    root_element: &RootElement,
    new_parent: &RootElement,
) -> Result<(), XmlMergerError> {
    for ns in &root_element.namespaces {
        if new_parent.namespaces.contains(ns) {
            // do not merge namespace if new parent already has it
            continue;
        }

        let mut key = String::from("xmlns");
        match ns.0 {
            Prefix::Default => {}
            Prefix::Named(ref n) => {
                key.push(':');
                key.push_str(n);
            }
        }

        // merge namespace if this chunk's start tag does not have it yet (and
        // does not overwrite it)
        if bytes_start.try_get_attribute(&key)?.is_none() {
            let val = escape(&ns.1);
            bytes_start.push_attribute((key.as_bytes(), val.as_bytes()));
        }
    }

    Ok(())
}

/// Merge the `schemaLocation` attribute of the given root element into the
/// given start tag. Do not overwrite schema locations that already exist in
/// the start tag, and don't unnecessarily add schema locations that already
/// exist in the new parent.
fn merge_schema_location_into_start_tag(
    bytes_start: &mut BytesStart,
    root_element: &RootElement,
    new_parent: &RootElement,
    reader: &Reader<&[u8]>,
) -> Result<(), XmlMergerError> {
    if let Some((ref slp, ref slv)) = root_element.schema_location {
        // find and remove existing schema location value
        let key = match slp {
            Prefix::Default => Cow::from("schemaLocation"),
            Prefix::Named(n) => Cow::from(format!("{}:schemaLocation", n)),
        };
        let qname = QName(key.as_bytes());
        let mut dummy = BytesStart::new("a");
        let mut existing_schema_location = None;
        for a in bytes_start.attributes() {
            let a = a.map_err(quick_xml::Error::InvalidAttr)?;
            if a.key == qname {
                existing_schema_location = Some(a.value);
            } else {
                dummy.push_attribute(a);
            }
        }

        // collect schema locations ...
        // start with the ones from the root element to merge
        let mut schema_locations = slv.clone();

        // remove those that are already defined in the new parent
        if let Some((_, ref npslv)) = new_parent.schema_location {
            for sl in npslv {
                let mut i = 0;
                while i < schema_locations.len() {
                    if schema_locations[i] == *sl {
                        schema_locations.remove(i);
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // overwrite schema locations from chunk
        if let Some(existing_schema_location) = existing_schema_location {
            let v = reader.decoder().decode(&existing_schema_location)?;
            let mut esl = Vec::new();
            if !v.is_empty() {
                for c in &v.split(' ').filter(|v| !v.is_empty()).chunks(2) {
                    if let Some((namespace, uri)) = c.collect_tuple() {
                        esl.push((namespace.to_string(), uri.to_string()));
                    } else {
                        break;
                    }
                }
            }

            for psl in esl {
                if let Some(i) = schema_locations.iter().position(|sl| sl.0 == psl.0) {
                    schema_locations[i].1 = psl.1;
                } else {
                    schema_locations.push(psl);
                }
            }
        }

        if !schema_locations.is_empty() {
            dummy.push_attribute((
                key.as_bytes(),
                schema_locations
                    .into_iter()
                    .flat_map(|(a, b)| vec![a, b])
                    .join(" ")
                    .as_bytes(),
            ));
        }

        // update bytes_start
        bytes_start.clear_attributes();
        for a in dummy.attributes() {
            let a = a.map_err(quick_xml::Error::InvalidAttr)?;
            bytes_start.push_attribute(a);
        }
    }

    Ok(())
}

/// Merge a root element into the given chunk by merging namespaces and
/// schemaLocation attributes. Consider possible namespaces and schemaLocation
/// attribute of the given new parent and don't unnecessarily add too much
/// information. Return a new start tag and the remainder of the chunk.
fn merge_root_element_into_chunk<'a>(
    chunk: &'a [u8],
    root_element: &RootElement,
    new_parent: &RootElement,
) -> Result<(BytesStart<'static>, Cow<'a, [u8]>), XmlMergerError> {
    // TODO shouldn't we consider encoding?
    let mut reader = Reader::from_reader(chunk);
    let (mut bytes_start, remainder) = split_chunk(chunk, &mut reader)?;

    merge_namespaces_into_start_tag(&mut bytes_start, root_element, new_parent)?;
    merge_schema_location_into_start_tag(&mut bytes_start, root_element, new_parent, &reader)?;

    Ok((bytes_start.to_owned(), remainder))
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use pretty_assertions::assert_eq;
    use ulid::Ulid;

    use crate::{
        index::{
            chunk_meta::ChunkMeta,
            gml::root_element::{Prefix, RootElement},
        },
        output::Merger,
    };

    use super::{XmlMerger, XmlMergerError};

    /// Test if simple chunks can be merged
    #[test]
    fn simple() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"></test>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(Prefix::Default, "root".to_string(), vec![], None),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(Prefix::Default, "root".to_string(), vec![], None),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        merger.merge(chunk2.as_bytes(), meta2).unwrap();
        merger.finish().unwrap();

        let s = from_utf8(&s).unwrap();
        let expected = format!("<root>{}{}</root>", chunk1, chunk2);
        assert_eq!(s, &*expected);
    }

    /// Test if simple chunks with the same namespaces can be merged
    #[test]
    fn simple_same_namespaces() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"></test>"#;

        let re = RootElement::new(
            Prefix::Default,
            "root".to_string(),
            vec![
                (Prefix::Default, "https://example.com".to_string()),
                (
                    Prefix::Named("foo".to_string()),
                    "https://foo.com".to_string(),
                ),
            ],
            None,
        );
        let meta1 = ChunkMeta::new(Ulid::new(), re.clone());
        let meta2 = ChunkMeta::new(Ulid::new(), re);

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        merger.merge(chunk2.as_bytes(), meta2).unwrap();
        merger.finish().unwrap();

        let s = from_utf8(&s).unwrap();
        let expected = format!(
            r#"<root xmlns="https://example.com" xmlns:foo="https://foo.com">{}{}</root>"#,
            chunk1, chunk2
        );
        assert_eq!(s, &*expected);
    }

    /// Test if two chunks with different namespaces but the same root element
    /// (with the same namespace and prefix) can be merged
    #[test]
    fn different_namespaces_same_root() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"></test>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix1".to_string()),
                        "https://prefix1.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix2".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        merger.merge(chunk2.as_bytes(), meta2).unwrap();
        merger.finish().unwrap();

        let s = from_utf8(&s).unwrap();
        let expected = concat!(
            r#"<foo:root xmlns:foo="https://foo.com" xmlns:prefix1="https://prefix1.com">"#,
            r#"<test chunk="1"></test>"#,
            r#"<test chunk="2" xmlns:prefix2="https://prefix2.com"></test>"#,
            r#"</foo:root>"#,
        );
        assert_eq!(s, expected);
    }

    /// Test if two chunks with different namespaces but the same root element
    /// name (but different prefix) can be merged
    #[test]
    fn different_namespaces_same_root_name() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"></test>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix1".to_string()),
                        "https://prefix1.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("bar".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix2".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("bar".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        merger.merge(chunk2.as_bytes(), meta2).unwrap();
        merger.finish().unwrap();

        let s = from_utf8(&s).unwrap();
        let expected = concat!(
            r#"<foo:root xmlns:foo="https://foo.com" xmlns:prefix1="https://prefix1.com">"#,
            r#"<test chunk="1"></test>"#,
            r#"<test chunk="2" xmlns:bar="https://foo.com" xmlns:prefix2="https://prefix2.com"></test>"#,
            r#"</foo:root>"#,
        );
        assert_eq!(s, expected);
    }

    /// Test if two chunks with different namespaces but the same root element
    /// (with the same namespace and prefix) can be merged even if the second
    /// chunk overwrites one of the namespaces
    #[test]
    fn different_namespaces_overwrite() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2" xmlns:foo="https://bar.com"></test>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix1".to_string()),
                        "https://prefix1.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix2".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        merger.merge(chunk2.as_bytes(), meta2).unwrap();
        merger.finish().unwrap();

        let s = from_utf8(&s).unwrap();
        let expected = concat!(
            r#"<foo:root xmlns:foo="https://foo.com" xmlns:prefix1="https://prefix1.com">"#,
            r#"<test chunk="1"></test>"#,
            r#"<test chunk="2" xmlns:foo="https://bar.com" xmlns:prefix2="https://prefix2.com"></test>"#,
            r#"</foo:root>"#,
        );
        assert_eq!(s, expected);
    }

    /// Test what happens if we try to merge two chunks but one of them has
    /// an empty tag instead of a start tag
    #[test]
    fn merge_empty_chunk() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"/>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix1".to_string()),
                        "https://prefix1.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix2".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        merger.merge(chunk2.as_bytes(), meta2).unwrap();
        merger.finish().unwrap();

        let s = from_utf8(&s).unwrap();
        let expected = concat!(
            r#"<foo:root xmlns:foo="https://foo.com" xmlns:prefix1="https://prefix1.com">"#,
            r#"<test chunk="1"></test>"#,
            r#"<test chunk="2" xmlns:prefix2="https://prefix2.com"></test>"#,
            r#"</foo:root>"#,
        );
        assert_eq!(s, expected);
    }

    /// Test what happens if we try to merge two chunks but one of them has
    /// an empty tag instead of a start tag followed by additional content
    #[test]
    fn merge_empty_chunk_with_additional_content() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = "<test chunk=\"2\"/>  \n   ";
        let chunk3 = r#"<test chunk="3"/><another tag />"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix1".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix2".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );
        let meta3 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix3".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();

        // this should be OK because the chunk's remainder contains whitespace only
        merger.merge(chunk2.as_bytes(), meta2).unwrap();

        let e = merger.merge(chunk3.as_bytes(), meta3).unwrap_err();
        assert!(matches!(e, XmlMergerError::UnexpectedContent));
    }

    /// Test what happens if we try to merge two chunks but one of them does
    /// neither have a start tag nor an empty tag
    #[test]
    fn merge_chunk_without_tag() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#""#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix1".to_string()),
                        "https://prefix1.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (
                        Prefix::Named("prefix2".to_string()),
                        "https://prefix2.com".to_string(),
                    ),
                    (
                        Prefix::Named("foo".to_string()),
                        "https://foo.com".to_string(),
                    ),
                ],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        let e = merger.merge(chunk2.as_bytes(), meta2).unwrap_err();
        assert!(matches!(e, XmlMergerError::PrematureEndOfChunk));
    }

    /// Test what happens if we try to merge two chunks with different root
    /// element names
    #[test]
    fn merge_root_elements_different_names() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"></test>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root1".to_string(),
                vec![(
                    Prefix::Named("foo".to_string()),
                    "https://foo.com".to_string(),
                )],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root2".to_string(),
                vec![(
                    Prefix::Named("foo".to_string()),
                    "https://foo.com".to_string(),
                )],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        let e = merger.merge(chunk2.as_bytes(), meta2).unwrap_err();
        match e {
            XmlMergerError::MultipleRootElements { found, expected } => {
                assert_eq!(found, "root2");
                assert_eq!(expected, "root1");
            }
            _ => panic!("Expected XmlMergerError::MultipleRootElements"),
        }
    }

    /// Test what happens if we try to merge two chunks that have root elements
    /// with the same name but from different namespaces
    #[test]
    fn merge_root_elements_different_namespaces() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"></test>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![(
                    Prefix::Named("foo".to_string()),
                    "https://foo.com".to_string(),
                )],
                None,
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![(
                    Prefix::Named("foo".to_string()),
                    "https://bar.com".to_string(),
                )],
                None,
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        let e = merger.merge(chunk2.as_bytes(), meta2).unwrap_err();
        match e {
            XmlMergerError::UnexpectedRootElementNamespace { found, expected } => {
                assert_eq!(found, "https://bar.com");
                assert_eq!(expected, "https://foo.com");
            }
            _ => panic!("Expected XmlMergerError::UnexpectedRootElementNamespace"),
        }
    }

    /// Test if chunks with different schema location attributes can be merged
    #[test]
    fn schema_location() {
        let chunk1 = r#"<test chunk="1"></test>"#;
        let chunk2 = r#"<test chunk="2"></test>"#;
        let chunk3 = r#"<test chunk="3"></test>"#;
        let chunk4 = r#"<test chunk="4"></test>"#;
        let chunk5 = r#"<test chunk="5" xsi:schemaLocation="_prefix1 https://yet-another-prefix1.com elvis http://elvis.com"></test>"#;

        let meta1 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (Prefix::Named("prefix1".to_string()), "_prefix1".to_string()),
                    (Prefix::Named("foo".to_string()), "_foo".to_string()),
                    (
                        Prefix::Named("xsi".to_string()),
                        "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                    ),
                ],
                Some((
                    Prefix::Named("xsi".to_string()),
                    vec![
                        ("_prefix1".to_string(), "https://prefix1.com".to_string()),
                        ("_foo".to_string(), "https://foo.com".to_string()),
                    ],
                )),
            ),
        );
        let meta2 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("bar".to_string()),
                "root".to_string(),
                vec![
                    (Prefix::Named("prefix2".to_string()), "_prefix2".to_string()),
                    (Prefix::Named("bar".to_string()), "_foo".to_string()),
                    (
                        Prefix::Named("xsi".to_string()),
                        "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                    ),
                ],
                Some((
                    Prefix::Named("xsi".to_string()),
                    vec![
                        ("_prefix2".to_string(), "https://prefix2.com".to_string()),
                        ("_foo".to_string(), "https://foo.com".to_string()),
                    ],
                )),
            ),
        );
        let meta3 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (Prefix::Named("foo".to_string()), "_foo".to_string()),
                    (
                        Prefix::Named("xsi".to_string()),
                        "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                    ),
                ],
                Some((
                    Prefix::Named("xsi".to_string()),
                    vec![("_foo".to_string(), "https://foo.com".to_string())],
                )),
            ),
        );
        let meta4 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (Prefix::Named("prefix1".to_string()), "_prefix1".to_string()),
                    (Prefix::Named("prefix4".to_string()), "_prefix4".to_string()),
                    (Prefix::Named("foo".to_string()), "_foo".to_string()),
                    (
                        Prefix::Named("xsi2".to_string()),
                        "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                    ),
                ],
                Some((
                    Prefix::Named("xsi2".to_string()),
                    vec![
                        (
                            "_prefix1".to_string(),
                            "https://another_prefix1.com".to_string(),
                        ),
                        ("_prefix4".to_string(), "https://prefix4.com".to_string()),
                        ("_foo".to_string(), "https://foo.com".to_string()),
                    ],
                )),
            ),
        );
        let meta5 = ChunkMeta::new(
            Ulid::new(),
            RootElement::new(
                Prefix::Named("foo".to_string()),
                "root".to_string(),
                vec![
                    (Prefix::Named("foo".to_string()), "_foo".to_string()),
                    (
                        Prefix::Named("xsi".to_string()),
                        "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                    ),
                ],
                Some((
                    Prefix::Named("xsi".to_string()),
                    vec![("_foo".to_string(), "https://foo2.com".to_string())],
                )),
            ),
        );

        let mut s = Vec::new();
        let mut merger = XmlMerger::new(&mut s);

        merger.merge(chunk1.as_bytes(), meta1).unwrap();
        merger.merge(chunk2.as_bytes(), meta2).unwrap();
        merger.merge(chunk3.as_bytes(), meta3).unwrap();
        merger.merge(chunk4.as_bytes(), meta4).unwrap();
        merger.merge(chunk5.as_bytes(), meta5).unwrap();
        merger.finish().unwrap();

        let s = from_utf8(&s).unwrap();
        let expected = concat!(
            r#"<foo:root xmlns:foo="_foo" xmlns:prefix1="_prefix1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="_foo https://foo.com _prefix1 https://prefix1.com">"#,
            r#"<test chunk="1"></test>"#,
            r#"<test chunk="2" xmlns:bar="_foo" xmlns:prefix2="_prefix2" xsi:schemaLocation="_prefix2 https://prefix2.com"></test>"#,
            r#"<test chunk="3"></test>"#,
            r#"<test chunk="4" xmlns:prefix4="_prefix4" xmlns:xsi2="http://www.w3.org/2001/XMLSchema-instance" xsi2:schemaLocation="_prefix1 https://another_prefix1.com _prefix4 https://prefix4.com"></test>"#,
            r#"<test chunk="5" xsi:schemaLocation="_foo https://foo2.com _prefix1 https://yet-another-prefix1.com elvis http://elvis.com"></test>"#,
            r#"</foo:root>"#,
        );
        assert_eq!(s, expected);
    }
}
