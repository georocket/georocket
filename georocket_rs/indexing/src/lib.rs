/// `types` contains the types necessary for indexing functionality across
/// georocket crates.
use serde::Deserialize;
use serde::Serialize;

pub mod bounding_box;
pub mod attributes;

#[derive(Serialize, Deserialize)]
enum Index {
    BoundingBox(bounding_box::BoundingBox),
    Attributes(attributes::Attributes),
}
