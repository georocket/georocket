use std::str::from_utf8;

use anyhow::Result;
use bincode::{Decode, Encode};
use quick_xml::{events::BytesStart, name::PrefixDeclaration, Reader};

/// An XML namespace prefix declaration
#[derive(PartialEq, Eq, Clone, Debug, Encode, Decode, Ord, PartialOrd)]
pub enum Prefix {
    Default,
    Named(String),
}

/// A list of XML namespaces
#[derive(PartialEq, Eq, Clone, Debug, Encode, Decode)]
pub struct Namespaces(pub Vec<(Prefix, String)>);

impl Namespaces {
    /// Extracts the namespaces from the given XML tag. The namespaces will be
    /// returned in lexicographical order.
    pub fn try_from_xml_tag<B>(tag: &BytesStart, reader: &Reader<B>) -> Result<Self> {
        let mut namespaces = Vec::new();

        for attr in tag.attributes() {
            let attr = attr?;
            if let Some(binding) = attr.key.as_namespace_binding() {
                let prefix = match binding {
                    PrefixDeclaration::Default => Prefix::Default,
                    PrefixDeclaration::Named(b"") => Prefix::Default,
                    PrefixDeclaration::Named(n) => Prefix::Named(from_utf8(n)?.to_string()),
                };
                let value = attr.decode_and_unescape_value(reader)?;
                namespaces.push((prefix, value.to_string()));
            }
        }

        namespaces.sort_unstable();

        Ok(Self(namespaces))
    }
}

#[cfg(test)]
mod tests {
    use quick_xml::{events::Event, Reader};

    use crate::index::gml::namespaces::{Namespaces, Prefix};

    #[test]
    fn simple() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" xmlns:foo="https://foo.com" xmlns="https://example.com" />"#;
        let mut reader = Reader::from_str(xml);

        let mut checked = false;
        let mut buf = Vec::new();
        loop {
            let e = reader.read_event_into(&mut buf).unwrap();
            match e {
                Event::Start(bs) | Event::Empty(bs) => {
                    let ns = Namespaces::try_from_xml_tag(&bs, &reader).unwrap();
                    assert_eq!(
                        ns,
                        Namespaces(vec![
                            (Prefix::Default, "https://example.com".to_string()),
                            (
                                Prefix::Named("foo".to_string()),
                                "https://foo.com".to_string()
                            ),
                            (
                                Prefix::Named("zoo".to_string()),
                                "https://zoo.com".to_string()
                            )
                        ])
                    );
                    checked = true;
                }
                Event::Eof => break,
                _ => {}
            }
            buf.clear();
        }

        assert!(checked);
    }
}
