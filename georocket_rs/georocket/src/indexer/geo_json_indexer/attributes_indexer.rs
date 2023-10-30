use crate::types::{IndexElement, Payload};
use actson::JsonEvent;
use indexing::attributes::AttributesBuilder;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AttributeIndexerError {
    #[error(
        "the AttributesBuilderHelper is in an invalid state to retrieve a valid Attributes value.\
    A key was added for and the AttributesBuilderHelper is waiting for the corresponding value."
    )]
    InvalidHelperState,
}
pub struct AttributesIndexer {
    inner: Inner,
}

impl AttributesIndexer {
    pub fn new() -> Self {
        Self {
            inner: Inner::new(),
        }
    }
    pub fn process_event(&mut self, event: (JsonEvent, Payload)) {
        self.inner.process_event(event);
    }
    pub fn retrieve_index_element(self) -> Result<Option<IndexElement>, AttributeIndexerError> {
        self.inner.retrieve_index_element()
    }
}

enum Inner {
    Uninitialized,
    Processing {
        current_level: usize,
        attribute_builder_helper: AttributesBuilderHelper,
    },
    Done {
        attribute_builder: Result<AttributesBuilder, AttributeIndexerError>,
    },
}

impl Inner {
    fn new() -> Self {
        Self::Uninitialized
    }
    fn increase_level(&mut self) {
        match self {
            Inner::Processing { current_level, .. } => {
                *current_level += 1;
            }
            _ => (),
        }
    }
    fn decrease_level(&mut self) {
        match self {
            Inner::Processing {
                current_level,
                attribute_builder_helper,
            } => {
                *current_level -= 1;
                if *current_level == 0 {
                    *self = Inner::Done {
                        attribute_builder: std::mem::take(attribute_builder_helper).into_inner(),
                    };
                }
            }
            _ => (),
        }
    }
    fn add_key(&mut self, key: String) {
        match self {
            Inner::Processing {
                attribute_builder_helper,
                current_level,
            } => {
                if *current_level == 1 {
                    *attribute_builder_helper =
                        std::mem::take(attribute_builder_helper).set_key(key);
                }
            }
            Inner::Uninitialized if key.as_str() == "properties" => {
                *self = Inner::Processing {
                    current_level: 0,
                    attribute_builder_helper: AttributesBuilderHelper::default(),
                }
            }
            _ => (),
        }
    }
    fn add_value(&mut self, value: String) {
        match self {
            Inner::Processing {
                attribute_builder_helper,
                current_level,
            } => {
                if *current_level == 1 {
                    *attribute_builder_helper =
                        std::mem::take(attribute_builder_helper).add_value(value);
                }
            }
            _ => (),
        }
    }
    fn process_event(&mut self, event: (JsonEvent, Payload)) {
        use JsonEvent as E;
        let (json_event, payload) = event;
        match json_event {
            E::StartArray | E::StartObject => self.increase_level(),
            E::EndArray | E::EndObject => self.decrease_level(),
            E::FieldName => self
                .add_key(payload.expect("Payload of JsonEvent::FiledName should always be Some")),
            E::ValueDouble | E::ValueInt | E::ValueString => self.add_value(payload.expect(
                "Payload of JsonEvent::{ValueDouble, ValueInt, ValueDouble} should always be Some",
            )),
            E::ValueFalse => self.add_value("false".to_string()),
            E::ValueTrue => self.add_value("true".to_string()),
            E::ValueNull => self.add_value("null".to_string()),
            _ => (),
        }
    }
    fn retrieve_index_element(self) -> Result<Option<IndexElement>, AttributeIndexerError> {
        match self {
            Inner::Uninitialized => Ok(None),
            Inner::Processing {
                attribute_builder_helper,
                ..
            } => {
                todo!()
            }
            Inner::Done { attribute_builder } => attribute_builder
                .map(|attribute_builder| Some(IndexElement::Attributes(attribute_builder.build()))),
        }
    }
}

enum AttributesBuilderHelper {
    WaitingForKey(AttributesBuilder),
    WaitingForValue(AttributesBuilder, String),
}

impl AttributesBuilderHelper {
    #[must_use]
    fn set_key(self, key: String) -> Self {
        match self {
            Self::WaitingForKey(attribute_builder)
            | Self::WaitingForValue(attribute_builder, ..) => {
                Self::WaitingForValue(attribute_builder, key)
            }
        }
    }
    #[must_use]
    fn add_value(self, value: String) -> Self {
        match self {
            AttributesBuilderHelper::WaitingForKey(_) => self,
            AttributesBuilderHelper::WaitingForValue(mut attributes_builder, key) => {
                attributes_builder = attributes_builder.add_attribute(key, value);
                Self::WaitingForKey(attributes_builder)
            }
        }
    }
    fn into_inner(self) -> Result<AttributesBuilder, AttributeIndexerError> {
        match self {
            Self::WaitingForKey(attributes_builder) => Ok(attributes_builder),
            Self::WaitingForValue(..) => Err(AttributeIndexerError::InvalidHelperState),
        }
    }
}

impl Default for AttributesBuilderHelper {
    fn default() -> Self {
        Self::WaitingForKey(AttributesBuilder::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Chunk, InnerChunk};
    use indexing::attributes::Value;
    use std::path::Path;

    #[test]
    fn helper_adding_key_values() {
        let mut helper = AttributesBuilderHelper::default();
        assert!(matches!(helper, AttributesBuilderHelper::WaitingForKey(_)));
        let key_values = [
            ("string".to_string(), "value".to_string()),
            ("int".to_string(), "1".to_string()),
            ("float".to_string(), "1.0".to_string()),
        ];
        for (key, value) in key_values.iter().cloned() {
            helper = helper.set_key(key);
            helper = helper.add_value(value);
        }
        let attributes = helper.into_inner().unwrap().build();
        assert_eq!(attributes.len(), 3);
        assert_eq!(
            attributes.get("string"),
            Some(&Value::String("value".to_string())),
        );
        assert_eq!(attributes.get("int"), Some(&Value::Integer(1)));
        assert_eq!(attributes.get("float"), Some(&Value::Double(1.0)));
    }

    #[test]
    fn inner_state_transitions() {
        use JsonEvent as E;
        let mut inner = Inner::new();
        assert!(matches!(inner, Inner::Uninitialized));
        inner.process_event((E::FieldName, Some("properties".into())));
        assert!(matches!(inner, Inner::Processing { .. }));
        inner.process_event((E::StartObject, None));
        inner.process_event((E::EndObject, None));
        assert!(matches!(inner, Inner::Done { .. }))
    }

    #[test]
    fn inner_multiple_key_vals() {
        use JsonEvent as E;
        let mut indexer = AttributesIndexer::new();
        indexer.process_event((E::FieldName, Some("properties".into())));
        indexer.process_event((E::StartObject, None));
        indexer.process_event((E::FieldName, Some("string_key".into())));
        indexer.process_event((E::ValueString, Some("string".into())));
        indexer.process_event((E::FieldName, Some("int_key".into())));
        indexer.process_event((E::ValueInt, Some("1".into())));
        indexer.process_event((E::FieldName, Some("float_key".into())));
        indexer.process_event((E::ValueInt, Some("1.0".into())));
        indexer.process_event((E::EndObject, None));
        if let IndexElement::Attributes(attributes) =
            indexer.retrieve_index_element().unwrap().unwrap()
        {
            assert_eq!(attributes.len(), 3);
            assert_eq!(
                attributes.get("string_key").unwrap(),
                &Value::String("string".into())
            );
            assert_eq!(attributes.get("int_key").unwrap(), &Value::Integer(1));
            assert_eq!(attributes.get("float_key").unwrap(), &Value::Double(1.0));
        }
    }

    fn test_helper(control: Vec<Vec<(&str, Value)>>, chunks: Vec<Chunk>) {
        for (chunk, key_vals) in chunks.into_iter().zip(control) {
            let InnerChunk::GeoJson(chunk) = chunk.inner;
            let mut attributes_indexer = AttributesIndexer::new();
            for (event, payload) in chunk {
                attributes_indexer.process_event((event, payload));
            }
            let index = attributes_indexer
                .retrieve_index_element()
                .unwrap()
                .unwrap();
            if let IndexElement::Attributes(attributes) = index {
                assert_eq!(key_vals.len(), attributes.len());
                for (key, value) in key_vals {
                    assert_eq!(attributes.get(key).unwrap(), &value);
                }
            } else {
                unreachable!("index must be IndexElement::Attributes")
            }
        }
    }

    #[tokio::test]
    async fn index_basic_attributes() {
        let control = vec![("name", Value::String("Dinagat Islands".into()))];
        let chunks = chunks_from_file("test_files/simple_feature_01.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn index_more_attributes() {
        let control = vec![
            ("name", Value::String("Dinagat Islands".into())),
            ("population", Value::Integer(32873)),
            ("ratio", Value::Double(0.51)),
        ];
        let chunks = chunks_from_file("test_files/simple_feature_02.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn index_true_false_null() {
        let control = vec![
            ("good", Value::String("false".into())),
            ("bad", Value::String("true".into())),
            ("blackjack", Value::String("null".into())),
        ];
        let chunks = chunks_from_file("test_files/simple_feature_03.json").await;
        test_helper(vec![control], chunks);
    }

    // #[tokio::test]
    // async fn index_nested_properties() {
    //     let control = vec![("prop0", Value::String("value0".into()))];
    //     let chunks = chunks_from_file("test_files/simple_feature_nested_properties_01.json").await;
    //     test_helper(vec![control], chunks);
    // }
    //
    // #[tokio::test]
    // async fn index_collection() {
    //     let control = vec![
    //         vec![("prop0", Value::String("value0".into()))],
    //         vec![
    //             ("prop0", Value::String("value0".into())),
    //             ("prop1", Value::Double(0.0)),
    //         ],
    //         vec![("prop0", Value::String("value0".into()))],
    //     ];
    //     let chunks = chunks_from_file("test_files/simple_collection_01.json").await;
    //     test_helper(control, chunks);
    // }

    async fn run_splitter_and_get_chunk_channel(
        json_features: impl AsRef<Path>,
    ) -> async_channel::Receiver<crate::types::Chunk> {
        let (splitter_channels, chunk_receiver, _raw) =
            crate::input::SplitterChannels::new_with_channels(10, 10);
        let feature = tokio::fs::File::open(json_features).await.unwrap();
        let splitter = crate::input::GeoJsonSplitter::new(feature, splitter_channels);
        let handle = tokio::spawn(splitter.run());
        let geo_type = handle.await.unwrap().unwrap();
        chunk_receiver
    }

    async fn get_chunks(receiver: async_channel::Receiver<crate::types::Chunk>) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        while let Ok(chunk) = receiver.recv().await {
            chunks.push(chunk)
        }
        chunks
    }

    async fn chunks_from_file(json_features: impl AsRef<Path>) -> Vec<Chunk> {
        let chunk_receiver = run_splitter_and_get_chunk_channel(json_features).await;
        get_chunks(chunk_receiver).await
    }
}
