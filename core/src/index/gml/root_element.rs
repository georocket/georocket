use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
};

use anyhow::Result;
use bincode::{Decode, Encode};
use itertools::Itertools;
use quick_xml::{
    escape::escape,
    events::{BytesEnd, BytesStart},
    name::PrefixDeclaration,
    Reader,
};

/// An XML namespace prefix declaration
#[derive(PartialEq, Eq, Clone, Debug, Encode, Decode, Ord, PartialOrd)]
pub enum Prefix {
    Default,
    Named(String),
}

/// An XML root element
#[derive(PartialEq, Eq, Clone, Debug, Encode, Decode)]
pub struct RootElement {
    /// The root element's prefix
    pub prefix: Prefix,

    /// The root element's name
    pub name: String,

    /// A list of XML namespaces
    pub namespaces: Vec<(Prefix, String)>,

    /// An optional `schemaLocation` attribute
    pub schema_location: Option<(Prefix, Vec<(String, String)>)>,
}

impl Display for Prefix {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Prefix::Default => write!(f, "<default>"),
            Prefix::Named(name) => write!(f, "{}", name),
        }
    }
}

impl RootElement {
    /// Creates a new `RootElement` object with the given name and namespaces
    pub fn new(
        prefix: Prefix,
        name: String,
        mut namespaces: Vec<(Prefix, String)>,
        mut schema_location: Option<(Prefix, Vec<(String, String)>)>,
    ) -> Self {
        namespaces.sort_unstable();
        if let Some(ref mut sl) = schema_location {
            sl.1.sort_unstable();
        }
        Self {
            prefix,
            name,
            namespaces,
            schema_location,
        }
    }

    /// Creates a `RootElement` object from the given XML tag. The root
    /// element's namespaces and the optional schema locations will be sorted
    /// lexicographically.
    pub fn try_from_xml_tag<B>(
        tag: &BytesStart,
        reader: &Reader<B>,
    ) -> Result<Self, quick_xml::Error> {
        let decoder = reader.decoder();
        let prefix = match tag.name().prefix() {
            Some(p) => Prefix::Named(decoder.decode(p.as_ref())?.to_string()),
            None => Prefix::Default,
        };
        let name = decoder.decode(tag.local_name().as_ref())?.to_string();

        let mut namespaces = Vec::new();
        let mut xsi: Option<Prefix> = None;

        for attr in tag.attributes() {
            let attr = attr?;
            if let Some(binding) = attr.key.as_namespace_binding() {
                let prefix = match binding {
                    PrefixDeclaration::Default => Prefix::Default,
                    PrefixDeclaration::Named(b"") => Prefix::Default,
                    PrefixDeclaration::Named(n) => Prefix::Named(decoder.decode(n)?.to_string()),
                };

                let value = attr.decode_and_unescape_value(reader)?;
                if value == "http://www.w3.org/2001/XMLSchema-instance" {
                    xsi = Some(prefix.clone());
                }

                namespaces.push((prefix, value.to_string()));
            }
        }

        let schema_location = if let Some(xsi) = xsi {
            let sln = match xsi {
                Prefix::Default => Cow::from("schemaLocation"),
                Prefix::Named(ref n) => Cow::from(format!("{}:schemaLocation", n)),
            };
            tag.try_get_attribute(sln.as_bytes())?
                .map::<Result<_, quick_xml::Error>, _>(|sl| {
                    let v = sl.decode_and_unescape_value(reader)?;
                    if v.is_empty() {
                        Ok((xsi, vec![]))
                    } else {
                        let mut r = Vec::new();
                        for c in &v.split(' ').filter(|v| !v.is_empty()).chunks(2) {
                            if let Some((namespace, uri)) = c.collect_tuple() {
                                r.push((namespace.to_string(), uri.to_string()));
                            } else {
                                break;
                            }
                        }
                        Ok((xsi, r))
                    }
                })
                .transpose()?
        } else {
            None
        };

        Ok(Self::new(prefix, name, namespaces, schema_location))
    }

    /// Create a [`BytesStart`] object from this root element
    pub fn to_bytes_start(&self) -> BytesStart<'static> {
        let mut str = match self.prefix {
            Prefix::Default => String::new(),
            Prefix::Named(ref n) => format!("{n}:").to_string(),
        };
        str.push_str(&self.name);

        // append namespaces
        if !self.namespaces.is_empty() {
            for (prefix, ns) in &self.namespaces {
                str.push(' ');
                str.push_str("xmlns");
                match prefix {
                    Prefix::Default => {}
                    Prefix::Named(n) => {
                        str.push(':');
                        str.push_str(n);
                    }
                }
                str.push_str("=\"");
                str.push_str(&escape(ns));
                str.push('"');
            }
        }

        // append schema location attribute
        if let Some(sl) = &self.schema_location {
            str.push(' ');

            match sl.0 {
                Prefix::Default => {}
                Prefix::Named(ref n) => {
                    str.push_str(n);
                    str.push(':');
                }
            }

            str.push_str("schemaLocation=\"");
            for (i, (ns, uri)) in sl.1.iter().enumerate() {
                if i > 0 {
                    str.push(' ');
                }
                str.push_str(&escape(ns));
                str.push(' ');
                str.push_str(&escape(uri));
            }
            str.push('"');
        }

        BytesStart::from_content(str, self.name.len())
    }

    /// Create a [`BytesEnd`] object from this root element
    pub fn to_bytes_end(&self) -> BytesEnd<'static> {
        let mut str = match self.prefix {
            Prefix::Default => String::new(),
            Prefix::Named(ref n) => format!("{n}:").to_string(),
        };
        str.push_str(&self.name);
        BytesEnd::new(str)
    }
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use pretty_assertions::assert_eq;
    use quick_xml::{events::Event, Reader};

    use crate::index::gml::root_element::{Prefix, RootElement};

    fn check(xml: &str, expected: RootElement) {
        let mut reader = Reader::from_str(xml);

        let mut checked = false;
        loop {
            let e = reader.read_event().unwrap();
            match e {
                Event::Start(bs) | Event::Empty(bs) => {
                    let re = RootElement::try_from_xml_tag(&bs, &reader).unwrap();
                    assert_eq!(re, expected);
                    checked = true;
                }
                Event::Eof => break,
                _ => {}
            }
        }

        assert!(checked);
    }

    #[test]
    fn plain() {
        let xml = r#"<root />"#;
        let expected = RootElement::new(Prefix::Default, "root".to_string(), vec![], None);
        check(xml, expected);
    }

    #[test]
    fn with_namespaces() {
        let xml = r#"<root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
        />"#;
        let expected = RootElement::new(
            Prefix::Default,
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
    fn with_namespaces_and_root_prefix() {
        let xml = r#"<foo:root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
        />"#;
        let expected = RootElement::new(
            Prefix::Named("foo".to_string()),
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
        let xml = r#"<foo:root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            schemaLocation="foo.com https://foo.com example.com https://example.com"
        />"#;
        let expected = RootElement::new(
            Prefix::Named("foo".to_string()),
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
        let xml = r#"<foo:root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="wrong"
            xsi:schemaLocation="foo.com https://foo.com example.com https://example.com"
        />"#;
        let expected = RootElement::new(
            Prefix::Named("foo".to_string()),
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
        let xml = r#"<foo:root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation=""
        />"#;
        let expected = RootElement::new(
            Prefix::Named("foo".to_string()),
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
            Some((Prefix::Named("xsi".to_string()), vec![])),
        );
        check(xml, expected);
    }

    #[test]
    fn schema_location_wrong_count() {
        let xml = r#"<foo:root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="foo.com https://foo.com example.com"
        />"#;
        let expected = RootElement::new(
            Prefix::Named("foo".to_string()),
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
            Some((
                Prefix::Named("xsi".to_string()),
                vec![("foo.com".to_string(), "https://foo.com".to_string())],
            )),
        );
        check(xml, expected);
    }

    #[test]
    fn full() {
        let xml = r#"<foo:root xmlns:zoo="https://zoo.com" 
            xmlns:foo="foo.com"
            xmlns="example.com"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="foo.com https://foo.com example.com https://example.com"
        />"#;
        let expected = RootElement::new(
            Prefix::Named("foo".to_string()),
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
            Some((
                Prefix::Named("xsi".to_string()),
                vec![
                    ("example.com".to_string(), "https://example.com".to_string()),
                    ("foo.com".to_string(), "https://foo.com".to_string()),
                ],
            )),
        );
        check(xml, expected);
    }

    #[test]
    fn into_bytes_start_plain() {
        let re = RootElement::new(Prefix::Default, "root".to_string(), vec![], None);
        let expected = "root";
        let bs = re.to_bytes_start();
        let raw_bs = from_utf8(&bs).unwrap();
        assert_eq!(raw_bs, expected);
    }

    #[test]
    fn into_bytes_start_with_namespaces() {
        let re = RootElement::new(
            Prefix::Default,
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
            None,
        );
        let expected = concat!(
            r#"root xmlns="example.com" "#,
            r#"xmlns:foo="foo.com" "#,
            r#"xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" "#,
            r#"xmlns:zoo="https://zoo.com""#,
        );

        let bs = re.to_bytes_start();
        let raw_bs = from_utf8(&bs).unwrap();
        assert_eq!(raw_bs, expected);
    }

    #[test]
    fn into_bytes_start_full() {
        let re = RootElement::new(
            Prefix::Named("foo".to_string()),
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
            Some((
                Prefix::Named("xsi".to_string()),
                vec![
                    ("example.com".to_string(), "https://example.com".to_string()),
                    ("foo.com".to_string(), "https://foo.com".to_string()),
                ],
            )),
        );
        let expected = concat!(
            r#"foo:root xmlns="example.com" "#,
            r#"xmlns:foo="foo.com" "#,
            r#"xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" "#,
            r#"xmlns:zoo="https://zoo.com" "#,
            r#"xsi:schemaLocation="example.com https://example.com foo.com https://foo.com""#,
        );

        let bs = re.to_bytes_start();
        let raw_bs = from_utf8(&bs).unwrap();
        assert_eq!(raw_bs, expected);
    }

    #[test]
    fn into_bytes_start_full_escape() {
        let re = RootElement::new(
            Prefix::Named("foo".to_string()),
            "root".to_string(),
            vec![
                (Prefix::Default, "example.com".to_string()),
                (Prefix::Named("foo".to_string()), "foo.com".to_string()),
                (
                    Prefix::Named("xsi".to_string()),
                    "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                ),
                (Prefix::Named("zoo".to_string()), "Tom&Jerry".to_string()),
            ],
            Some((
                Prefix::Named("xsi".to_string()),
                vec![
                    ("example.com".to_string(), "https://example.com".to_string()),
                    ("foo.com".to_string(), "https://foo.com".to_string()),
                    ("Tom&Jerry".to_string(), "https://example.com".to_string()),
                ],
            )),
        );
        let expected = concat!(
            r#"foo:root xmlns="example.com" "#,
            r#"xmlns:foo="foo.com" "#,
            r#"xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" "#,
            r#"xmlns:zoo="Tom&amp;Jerry" "#,
            r#"xsi:schemaLocation="Tom&amp;Jerry https://example.com example.com https://example.com foo.com https://foo.com""#,
        );

        let bs = re.to_bytes_start();
        let raw_bs = from_utf8(&bs).unwrap();
        assert_eq!(raw_bs, expected);
    }
}
