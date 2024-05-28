use std::{collections::HashMap, mem, str::from_utf8};

use anyhow::Result;
use quick_xml::events::{attributes::Attribute, BytesStart, Event};

use crate::index::{IndexedValue, Indexer, Value};

/// Indexer for CityGML generic attributes
#[derive(Default)]
pub struct GenericAttributeIndexer {
    /// The key of the currently parsed generic attribute
    current_key: Option<String>,

    /// `true` if we're currently parsing a value of a generic attribute
    parsing_value: bool,

    /// A map collecting all attributes parsed
    result: HashMap<String, Value>,
}

impl<'a> Indexer<Event<'a>> for GenericAttributeIndexer {
    fn on_event(&mut self, event: &Event<'a>) -> Result<()> {
        match event {
            Event::Start(s) => {
                let local_name = s.local_name();
                if GenericAttributeIndexer::is_attribute_supported(local_name.as_ref()) {
                    if let Some(n) =
                        GenericAttributeIndexer::get_attribute_by_local_name(s, b"name")?
                    {
                        self.current_key = Some(from_utf8(&n.value)?.to_string());
                    } else {
                        self.current_key = None;
                    }
                } else if local_name.as_ref() == b"value" {
                    self.parsing_value = true
                }
            }

            Event::End(e) => {
                let local_name = e.local_name();
                if GenericAttributeIndexer::is_attribute_supported(local_name.as_ref()) {
                    self.current_key = None
                } else if local_name.as_ref() == b"value" {
                    self.parsing_value = false
                }
            }

            Event::Text(t) => {
                if self.parsing_value {
                    if let Some(key) = self.current_key.take() {
                        self.put(key, &t.unescape()?);
                    }
                }
            }

            Event::CData(d) => {
                if self.parsing_value {
                    if let Some(key) = self.current_key.take() {
                        self.put(key, from_utf8(d)?);
                    }
                }
            }

            _ => {}
        }

        Ok(())
    }

    fn make_result(&mut self) -> Vec<IndexedValue> {
        let mut new_result = HashMap::new();
        mem::swap(&mut self.result, &mut new_result);
        vec![IndexedValue::GenericAttributes(new_result)]
    }
}

impl GenericAttributeIndexer {
    fn get_attribute_by_local_name<'a>(
        start_tag: &'a BytesStart,
        name: &[u8],
    ) -> Result<Option<Attribute<'a>>> {
        for a in start_tag.attributes().with_checks(false) {
            let a = a?;
            if a.key.local_name().as_ref() == name {
                return Ok(Some(a));
            }
        }
        Ok(None)
    }

    fn is_attribute_supported(local_name: &[u8]) -> bool {
        local_name == b"stringAttribute"
            || local_name == b"intAttribute"
            || local_name == b"doubleAttribute"
            || local_name == b"dateAttribute"
            || local_name == b"uriAttribute"
            || local_name == b"measureAttribute"
    }

    fn put(&mut self, key: String, value: &str) {
        // auto-convert to numbers
        let value = if let Ok(n) = value.parse::<i64>() {
            Value::Integer(n)
        } else if let Ok(f) = value.parse::<f64>() {
            Value::Float(f)
        } else {
            Value::String(value.to_string())
        };

        // never overwrite attributes already collected!
        self.result.entry(key).or_insert(value);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use quick_xml::events::{BytesCData, BytesEnd, BytesStart, BytesText, Event};

    use crate::index::{IndexedValue, Indexer};

    use super::GenericAttributeIndexer;

    #[test]
    fn empty() {
        let mut i = GenericAttributeIndexer::default();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(HashMap::new())]
        );
    }

    #[test]
    fn empty_object() {
        let mut i = GenericAttributeIndexer::default();
        i.on_event(&Event::Start(BytesStart::new("object")))
            .unwrap();
        i.on_event(&Event::End(BytesEnd::new("object"))).unwrap();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(HashMap::new())]
        );
    }

    #[test]
    fn attribute_without_name() {
        let mut i = GenericAttributeIndexer::default();
        i.on_event(&Event::Start(BytesStart::new("object")))
            .unwrap();
        i.on_event(&Event::Start(BytesStart::new("stringAttribute")))
            .unwrap();
        i.on_event(&Event::End(BytesEnd::new("stringAttribute")))
            .unwrap();
        i.on_event(&Event::End(BytesEnd::new("object"))).unwrap();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(HashMap::new())]
        );
    }

    #[test]
    fn attribute_without_value() {
        let mut i = GenericAttributeIndexer::default();
        i.on_event(&Event::Start(BytesStart::new("object")))
            .unwrap();
        i.on_event(&Event::Start(BytesStart::from_content(
            "stringAttribute name=\"foo\"",
            15,
        )))
        .unwrap();
        i.on_event(&Event::End(BytesEnd::new("stringAttribute")))
            .unwrap();
        i.on_event(&Event::End(BytesEnd::new("object"))).unwrap();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(HashMap::new())]
        );
    }

    #[test]
    fn value_without_text() {
        let mut i = GenericAttributeIndexer::default();
        i.on_event(&Event::Start(BytesStart::new("object")))
            .unwrap();
        i.on_event(&Event::Start(BytesStart::from_content(
            "stringAttribute name=\"foo\"",
            15,
        )))
        .unwrap();
        i.on_event(&Event::Start(BytesStart::new("value"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("value"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("stringAttribute")))
            .unwrap();
        i.on_event(&Event::End(BytesEnd::new("object"))).unwrap();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(HashMap::new())]
        );
    }

    #[test]
    fn attribute_with_text() {
        let mut i = GenericAttributeIndexer::default();
        i.on_event(&Event::Start(BytesStart::new("object")))
            .unwrap();
        i.on_event(&Event::Start(BytesStart::from_content(
            "stringAttribute name=\"foo\"",
            15,
        )))
        .unwrap();
        i.on_event(&Event::Start(BytesStart::new("value"))).unwrap();
        i.on_event(&Event::Text(BytesText::new("bar"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("value"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("stringAttribute")))
            .unwrap();
        i.on_event(&Event::End(BytesEnd::new("object"))).unwrap();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(
                [("foo".to_string(), "bar".into())].into()
            )]
        );
    }

    #[test]
    fn attribute_with_cdata() {
        let mut i = GenericAttributeIndexer::default();
        i.on_event(&Event::Start(BytesStart::new("object")))
            .unwrap();
        i.on_event(&Event::Start(BytesStart::from_content(
            "stringAttribute name=\"foo\"",
            15,
        )))
        .unwrap();
        i.on_event(&Event::Start(BytesStart::new("value"))).unwrap();
        i.on_event(&Event::CData(BytesCData::new("bar"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("value"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("stringAttribute")))
            .unwrap();
        i.on_event(&Event::End(BytesEnd::new("object"))).unwrap();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(
                [("foo".to_string(), "bar".into())].into()
            )]
        );
    }

    #[test]
    fn multiple_attributes() {
        let mut i = GenericAttributeIndexer::default();
        i.on_event(&Event::Start(BytesStart::new("object")))
            .unwrap();

        i.on_event(&Event::Start(BytesStart::from_content(
            "stringAttribute name=\"foo\"",
            15,
        )))
        .unwrap();
        i.on_event(&Event::Start(BytesStart::new("value"))).unwrap();
        i.on_event(&Event::CData(BytesCData::new("bar"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("value"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("stringAttribute")))
            .unwrap();

        i.on_event(&Event::Start(BytesStart::from_content(
            "intAttribute name=\"height\"",
            12,
        )))
        .unwrap();
        i.on_event(&Event::Start(BytesStart::new("value"))).unwrap();
        i.on_event(&Event::CData(BytesCData::new("5"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("value"))).unwrap();
        i.on_event(&Event::End(BytesEnd::new("intAttribute")))
            .unwrap();

        i.on_event(&Event::End(BytesEnd::new("object"))).unwrap();
        assert_eq!(
            i.make_result(),
            vec![IndexedValue::GenericAttributes(
                [
                    ("height".to_string(), 5.into()),
                    ("foo".to_string(), "bar".into())
                ]
                .into()
            )]
        );
    }
}
