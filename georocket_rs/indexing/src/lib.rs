/// `types` contains the types necessary for indexing functionality across
/// georocket crates.
use serde::Deserialize;
use serde::Serialize;

use georocket_types::BoundingBox;

pub mod attributes;
pub mod bounding_box;

#[derive(Serialize, Deserialize)]
enum Index {
    BoundingBox(BoundingBox),
    Attributes(attributes::Attributes),
}
