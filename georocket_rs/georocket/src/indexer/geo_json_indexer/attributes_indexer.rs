use crate::types::{IndexElement, Payload};
use actson::JsonEvent;
use indexing::attributes::AttributesBuilder;
use std::fmt::{write, Display, Formatter};
use thiserror::Error;

mod key;
use key::Key;

#[derive(Error, Debug)]
pub enum AttributeIndexerError {
    #[error("AttributesIndexer needs to be in state Uninit or Done, but is in state {0}")]
    InvalidState(State),
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
    pub fn process_event(&mut self, json_event: JsonEvent, payload: Option<&str>) {
        self.inner.process_event(json_event, payload);
    }
    pub fn retrieve_index_element(self) -> Result<Option<IndexElement>, AttributeIndexerError> {
        self.inner.retrieve_index_element()
    }
}

#[derive(Copy, Clone, Debug)]
pub enum State {
    Uninit,
    Start,
    P,
    A,
    Done,
}

impl Display for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                State::Uninit => "State::Uninit",
                State::Start => "State::Start",
                State::P => "State::P",
                State::A => "State::A",
                State::Done => "State::Done",
            }
        )
    }
}

enum StackAlphabet {
    Field,
    Array(usize),
}

use StackAlphabet as SA;

struct Inner {
    state: State,
    key: Key,
    stack: Vec<StackAlphabet>,
    builder: AttributesBuilder,
}

impl Inner {
    fn new() -> Self {
        Self {
            state: State::Uninit,
            key: Key::new(),
            stack: Vec::new(),
            builder: AttributesBuilder::new(),
        }
    }
    fn process_event(&mut self, json_event: JsonEvent, payload: Option<&str>) {
        use JsonEvent as E;
        match json_event {
            E::StartObject => {
                self.process_start_object();
            }
            E::EndObject => self.process_end_object(),
            E::StartArray => self.process_start_array(),
            E::EndArray => self.process_end_array(),
            E::FieldName => self.process_field_name(
                payload.expect("Payload for 'E::FieldName' should always be Some"),
            ),
            E::ValueDouble | E::ValueInt | E::ValueString => self.process_value(payload.expect(
                "Payload of JsonEvent::{ValueDouble, ValueInt, ValueDouble} should always be Some",
            )),
            E::ValueFalse => self.process_value("false"),
            E::ValueTrue => self.process_value("true"),
            E::ValueNull => self.process_value("null"),
            _ => (),
        }
    }
    fn process_start_object(&mut self) {
        match self.state {
            State::Uninit => (),
            State::Start => {
                self.state = State::P;
            }
            State::P => (),
            State::A => {
                self.state = State::P;
            }
            State::Done => (),
        }
    }
    fn process_end_object(&mut self) {
        match self.state {
            State::Uninit => (),
            State::Start => (),
            State::P => {
                match self.stack.pop() {
                    None => {
                        self.state = State::Done;
                    }
                    Some(StackAlphabet::Field) => {
                        // done with processing this Field : Value pair, pop the key component
                        // from the key.
                        self.state = State::P;
                        self.key.pop();
                    }
                    Some(StackAlphabet::Array(n)) => {
                        // object being processed at array position n is done, increment array position
                        self.state = State::A;
                        self.stack.push(StackAlphabet::Array(n + 1));
                        self.key.pop().push(n + 1);
                    }
                }
            }
            State::A => unreachable!("Should not receive JsonEvent::EndObject in this state"),
            State::Done => (),
        }
    }
    fn process_start_array(&mut self) {
        match self.state {
            State::Uninit => (),
            State::Start => (),
            State::P => (),
            State::A => {
                self.state = State::A;
                self.stack.push(StackAlphabet::Array(0));
                self.key.push(0);
            }
            State::Done => (),
        }
    }
    fn process_end_array(&mut self) {
        match self.state {
            State::Uninit => (),
            State::Start => (),
            State::P => (),
            State::A => {
                // pop key component representing index of current array which has ended
                self.key.pop();
                match self.stack.pop().expect("If we are leaving an array, there must be a StackAlphabet::Array at the top of the stack") {
                    StackAlphabet::Array(_) => match self.stack.pop().expect("If we are leaving an array, we must either be in another array, or be the value to a corresponding field") {
                        StackAlphabet::Array(n) => {
                            // stay in state and increment the key component of the enclosing
                            // array as well as the index counter on the stack
                            self.state = State::A;
                            self.stack.push(StackAlphabet::Array(n + 1));
                            self.key.pop().push(n + 1);
                        }
                        StackAlphabet::Field => {
                            // move to State::P and remove the key component corresponding
                            // to the current (key, value) pair. The closed array was the value
                            // corresponding tot he field name
                            self.state = State::P;
                            self.key.pop();
                        }
                    },
                    StackAlphabet::Field => unreachable!("Can only end an array, if we were in one, in which case StackAlphabet::Array(_) should have been on top of the stack"),
                }
            }
            State::Done => (),
        }
    }
    fn process_field_name(&mut self, field_name: impl AsRef<str>) {
        let field_name: &str = field_name.as_ref();
        match self.state {
            State::Uninit => {
                if field_name == "properties" {
                    self.state = State::Start;
                }
            }
            State::Start | State::A => {
                unreachable!("Can only receive JsonEvent::FieldName in the State::P")
            }
            State::P => {
                // go to A and push the field name onto the key
                self.state = State::A;
                self.key.push(field_name);
                self.stack.push(StackAlphabet::Field);
            }
            State::Done => (),
        }
    }
    fn process_value(&mut self, value: impl AsRef<str>) {
        let value: &str = value.as_ref();
        match self.state {
            State::Uninit => (),
            State::Start => {
                unreachable!("State::Start must always be followed by JsonEvent::StartObject")
            }
            State::P => unreachable!("A value must be preceded by a field name or be the element of an array, in which case we should be in State::A"),
            State::A => {
                match self
                    .stack
                    .pop()
                    .expect("There must always be something on the stack, if we are in State::A")
                {
                    StackAlphabet::Field => {
                        self.state = State::P;
                        // add the (key, value) pair to the builder and pop the key component
                        // corresponding to the field name from the key
                        self.builder = std::mem::take(&mut self.builder).add_attribute(self.key.as_ref(), value.into());
                        self.key.pop();
                    }
                    StackAlphabet::Array(n) => {
                        // stay in state a and push StackAlphabet::Array(n+1) back onto the stack,
                        // because after adding this value we are at the next array index
                        self.state = State::A;
                        self.stack.push(StackAlphabet::Array(n+1));
                        self.builder.add_attribute_mut(self.key.as_ref(), value.into());// = std::mem::take(&mut self.builder).add_attribute(self.key.as_ref(), value.into());
                        // remove the key component corresponding to the index of the value added to the builder
                        // and replace it with the next index
                        self.key.pop().push(n+1);
                    }
                }
            }
            State::Done => {}
        }
    }
    fn retrieve_index_element(self) -> Result<Option<IndexElement>, AttributeIndexerError> {
        match self.state {
            State::Uninit => Ok(None),
            State::Done => Ok(Some(IndexElement::Attributes(self.builder.build()))),
            _ => Err(AttributeIndexerError::InvalidState(self.state)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Chunk, InnerChunk};
    use actson::JsonEvent::ValueString;
    use indexing::attributes::Value;
    use std::path::Path;

    #[test]
    fn inner_multiple_key_vals() {
        use JsonEvent as E;
        let mut indexer = AttributesIndexer::new();
        indexer.process_event(E::FieldName, Some("properties"));
        indexer.process_event(E::StartObject, None);
        indexer.process_event(E::FieldName, Some("string_key"));
        indexer.process_event(E::ValueString, Some("string".into()));
        indexer.process_event(E::FieldName, Some("int_key"));
        indexer.process_event(E::ValueInt, Some("1"));
        indexer.process_event(E::FieldName, Some("float_key"));
        indexer.process_event(E::ValueInt, Some("1.0"));
        indexer.process_event(E::EndObject, None);
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
                let payload = payload.as_ref().map(String::as_str);
                attributes_indexer.process_event(event, payload);
            }
            let index = attributes_indexer
                .retrieve_index_element()
                .unwrap()
                .unwrap();
            if let IndexElement::Attributes(attributes) = index {
                assert_eq!(key_vals.len(), attributes.len());
                for (key, value) in key_vals {
                    assert_eq!(
                        attributes.get(key).expect(
                            format!("key: '{}' not found in attributes: '{:?}'", key, attributes)
                                .as_str()
                        ),
                        &value
                    );
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

    #[tokio::test]
    async fn index_array() {
        let control = vec![
            ("array.0", Value::Integer(1)),
            ("array.1", Value::Integer(2)),
        ];
        let chunks = chunks_from_file("test_files/array_as_property.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn index_nested_properties() {
        let control = vec![
            ("prop0", Value::String("value0".into())),
            ("prop1.this", Value::String("that".into())),
        ];
        let chunks = chunks_from_file("test_files/nested_properties_feature_01.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn index_object_in_array() {
        let control = vec![
            ("array.0.val_0", Value::Integer(0)),
            ("array.1", Value::Integer(1)),
        ];
        let chunks = chunks_from_file("test_files/nested_object_in_array.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn index_object_in_object() {
        let control = vec![
            ("object1.object2.val_1", Value::Integer(1)),
            ("object1.val_2", Value::Integer(2)),
        ];
        let chunks = chunks_from_file("test_files/nested_object_in_object.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn index_object_in_object_in_array() {
        let control = vec![
            ("array.0.object1.object2.val_1", Value::Integer(1)),
            ("array.0.object1.val_2", Value::Integer(2)),
            ("array.1", Value::Integer(3)),
        ];
        let chunks = chunks_from_file("test_files/nested_object_in_object_in_array.json").await;
        test_helper(vec![control], chunks);
    }
    #[tokio::test]
    async fn more_nesting() {
        let control = vec![
            ("array.0.val_0.0", Value::Integer(1)),
            ("array.0.val_0.1", Value::Integer(2)),
            ("array.0.val_0.2", Value::Integer(3)),
            ("array.0.val_1.val_2", Value::String("true".into())),
            ("array.1", Value::Integer(1)),
            ("array.2", Value::Integer(2)),
            ("array.3", Value::Integer(3)),
            ("array.4.0", Value::Integer(1)),
            ("array.4.1", Value::Integer(2)),
            ("array.4.2.val_3.0", Value::Integer(1)),
            ("array.4.2.val_3.1", Value::Integer(2)),
            ("val_4", Value::String("null".into())),
        ];
        let chunks = chunks_from_file("test_files/nested_properties_feature_03.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn deeper_nesting() {
        let control = vec![
            ("base.val0_0", Value::String("null".into())),
            ("base.nest1.val1_0", Value::Integer(1)),
            ("base.nest1.nest2.val2_0", Value::Double(2.0)),
            ("base.nest1.nest2.nest3.val3", Value::String("three".into())),
            ("base.nest1.nest2.val2_1", Value::Double(4.0)),
            ("base.nest1.val1_1", Value::Integer(5)),
            ("base.val0_1", Value::String("true".into())),
        ];
        let chunks = chunks_from_file("test_files/nested_properties_feature_02.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn dots_in_keys() {
        let control = vec![
            ("ba.se.val0.0", Value::String("null".into())),
            ("ba.se.nest.1.val1.0", Value::Integer(1)),
            ("ba.se.nest.1.val1.1", Value::Integer(5)),
            ("ba.se.val0.1", Value::String("true".into())),
        ];
        let chunks = chunks_from_file("test_files/nested_properties_dots_in_keys.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn array() {
        let control = vec![
            ("array.0", Value::Integer(1)),
            ("array.1.0", Value::Integer(1)),
            ("array.1.1", Value::Integer(2)),
            ("array.2.field", Value::Integer(5)),
        ];
        let chunks = chunks_from_file("test_files/properties_array_feature_01.json").await;
        test_helper(vec![control], chunks);
    }

    #[tokio::test]
    async fn index_collection() {
        let control = vec![
            vec![("prop0", Value::String("value0".into()))],
            vec![
                ("prop0", Value::String("value0".into())),
                ("prop1", Value::Double(0.0)),
            ],
            vec![
                ("prop0", Value::String("value0".into())),
                ("prop1.this", Value::String("that".into())),
            ],
        ];
        let chunks = chunks_from_file("test_files/simple_collection_01.json").await;
        test_helper(control, chunks);
    }

    async fn run_splitter_and_get_chunk_channel(
        json_features: impl AsRef<Path>,
    ) -> async_channel::Receiver<crate::types::Chunk> {
        let (splitter_channels, chunk_receiver, _raw) =
            crate::input::SplitterChannels::new_with_channels(10, 10);
        let feature = tokio::fs::File::open(json_features).await.unwrap();
        let mut splitter = crate::input::GeoJsonSplitter::new(feature, splitter_channels);
        let handle = tokio::spawn(async move { splitter.run().await });
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
