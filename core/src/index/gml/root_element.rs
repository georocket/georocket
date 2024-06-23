use std::{borrow::Cow, str::from_utf8};

use anyhow::Result;
use bincode::{Decode, Encode};
use itertools::Itertools;
use quick_xml::{events::BytesStart, name::PrefixDeclaration, Reader};

/// An XML namespace prefix declaration
#[derive(PartialEq, Eq, Clone, Debug, Encode, Decode, Ord, PartialOrd)]
pub enum Prefix {
    Default,
    Named(String),
}

/// An XML root element
#[derive(PartialEq, Eq, Clone, Debug, Encode, Decode)]
pub struct RootElement {
    /// The root element's name
    pub name: String,

    /// A list of XML namespaces
    pub namespaces: Vec<(Prefix, String)>,

    /// An optional `schemaLocation` attribute
    pub schema_location: Option<Vec<(String, String)>>,
}

impl RootElement {
    /// Creates a new `RootElement` object with the given name and namespaces
    pub fn new(
        name: String,
        namespaces: Vec<(Prefix, String)>,
        schema_location: Option<Vec<(String, String)>>,
    ) -> Self {
        Self {
            name,
            namespaces,
            schema_location,
        }
    }

    /// Creates a `RootElement` object from the given XML tag. The root
    /// element's namespaces and the optional schema locations will be sorted
    /// lexicographically.
    pub fn try_from_xml_tag<B>(tag: &BytesStart, reader: &Reader<B>) -> Result<Self> {
        let name = from_utf8(tag.name().0)?.to_string();

        let mut namespaces = Vec::new();
        let mut xsi: Option<Prefix> = None;

        for attr in tag.attributes() {
            let attr = attr?;
            if let Some(binding) = attr.key.as_namespace_binding() {
                let prefix = match binding {
                    PrefixDeclaration::Default => Prefix::Default,
                    PrefixDeclaration::Named(b"") => Prefix::Default,
                    PrefixDeclaration::Named(n) => Prefix::Named(from_utf8(n)?.to_string()),
                };

                let value = attr.decode_and_unescape_value(reader)?;
                if value == "http://www.w3.org/2001/XMLSchema-instance" {
                    xsi = Some(prefix.clone());
                }

                namespaces.push((prefix, value.to_string()));
            }
        }

        namespaces.sort_unstable();

        let schema_location = if let Some(xsi) = xsi {
            let sln = match xsi {
                Prefix::Default => Cow::from("schemaLocation"),
                Prefix::Named(n) => Cow::from(format!("{}:schemaLocation", n)),
            };
            tag.try_get_attribute(sln.as_bytes())?
                .map(|sl| {
                    let v = from_utf8(&sl.value)?;
                    if v.is_empty() {
                        anyhow::Ok(vec![])
                    } else {
                        let mut r = Vec::new();
                        for c in &v.split(' ').filter(|v| !v.is_empty()).chunks(2) {
                            if let Some((namespace, uri)) = c.collect_tuple() {
                                r.push((namespace.to_string(), uri.to_string()));
                            } else {
                                break;
                            }
                        }
                        r.sort_unstable();
                        anyhow::Ok(r)
                    }
                })
                .transpose()?
        } else {
            None
        };

        Ok(Self {
            name,
            namespaces,
            schema_location,
        })
    }
}

#[cfg(test)]
mod tests {
    use assertor::{assert_that, EqualityAssertion};
    use quick_xml::{events::Event, Reader};

    use crate::index::gml::root_element::{Prefix, RootElement};

    fn check(xml: &str, expected: RootElement) {
        let mut reader = Reader::from_str(xml);

        let mut checked = false;
        let mut buf = Vec::new();
        loop {
            let e = reader.read_event_into(&mut buf).unwrap();
            match e {
                Event::Start(bs) | Event::Empty(bs) => {
                    let re = RootElement::try_from_xml_tag(&bs, &reader).unwrap();
                    assert_that!(re).is_equal_to(&expected);
                    checked = true;
                }
                Event::Eof => break,
                _ => {}
            }
            buf.clear();
        }

        assert!(checked);
    }

    #[test]
    fn plain() {
        let xml = r#"<root />"#;
        let expected = RootElement::new("root".to_string(), vec![], None);
        check(xml, expected);
    }

    #[test]
    fn with_namespaces() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
        />"#;
        let expected = RootElement::new(
            "root".to_string(),
            vec![
                (Prefix::Default, "example.com".to_string()),
                (Prefix::Named("foo".to_string()), "foo.com".to_string()),
                (
                    Prefix::Named("zoo".to_string()),
                    "https://zoo.com".to_string(),
                ),
            ],
            None,
        );
        check(xml, expected);
    }

    #[test]
    fn schema_location_without_prefix() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            schemaLocation="foo.com https://foo.com example.com https://example.com"
        />"#;
        let expected = RootElement::new(
            "root".to_string(),
            vec![
                (Prefix::Default, "example.com".to_string()),
                (Prefix::Named("foo".to_string()), "foo.com".to_string()),
                (
                    Prefix::Named("zoo".to_string()),
                    "https://zoo.com".to_string(),
                ),
            ],
            None,
        );
        check(xml, expected);
    }

    #[test]
    fn schema_location_wrong_namespace() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="wrong"
            xsi:schemaLocation="foo.com https://foo.com example.com https://example.com"
        />"#;
        let expected = RootElement::new(
            "root".to_string(),
            vec![
                (Prefix::Default, "example.com".to_string()),
                (Prefix::Named("foo".to_string()), "foo.com".to_string()),
                (Prefix::Named("xsi".to_string()), "wrong".to_string()),
                (
                    Prefix::Named("zoo".to_string()),
                    "https://zoo.com".to_string(),
                ),
            ],
            None,
        );
        check(xml, expected);
    }

    #[test]
    fn schema_location_empty() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation=""
        />"#;
        let expected = RootElement::new(
            "root".to_string(),
            vec![
                (Prefix::Default, "example.com".to_string()),
                (Prefix::Named("foo".to_string()), "foo.com".to_string()),
                (
                    Prefix::Named("xsi".to_string()),
                    "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                ),
                (
                    Prefix::Named("zoo".to_string()),
                    "https://zoo.com".to_string(),
                ),
            ],
            Some(vec![]),
        );
        check(xml, expected);
    }

    #[test]
    fn schema_location_wrong_count() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="foo.com https://foo.com example.com"
        />"#;
        let expected = RootElement::new(
            "root".to_string(),
            vec![
                (Prefix::Default, "example.com".to_string()),
                (Prefix::Named("foo".to_string()), "foo.com".to_string()),
                (
                    Prefix::Named("xsi".to_string()),
                    "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                ),
                (
                    Prefix::Named("zoo".to_string()),
                    "https://zoo.com".to_string(),
                ),
            ],
            Some(vec![("foo.com".to_string(), "https://foo.com".to_string())]),
        );
        check(xml, expected);
    }

    #[test]
    fn full() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="foo.com https://foo.com example.com https://example.com"
        />"#;
        let expected = RootElement::new(
            "root".to_string(),
            vec![
                (Prefix::Default, "example.com".to_string()),
                (Prefix::Named("foo".to_string()), "foo.com".to_string()),
                (
                    Prefix::Named("xsi".to_string()),
                    "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                ),
                (
                    Prefix::Named("zoo".to_string()),
                    "https://zoo.com".to_string(),
                ),
            ],
            Some(vec![
                ("example.com".to_string(), "https://example.com".to_string()),
                ("foo.com".to_string(), "https://foo.com".to_string()),
            ]),
        );
        check(xml, expected);
    }
}
