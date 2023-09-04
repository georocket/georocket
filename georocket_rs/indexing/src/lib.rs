/// `types` contains the types necessary for indexing functionality across
/// georocket crates.
use serde::Deserialize;
use serde::Serialize;

pub mod bounding_box;

#[derive(Serialize, Deserialize)]
enum Index {
    BoundingBox(bounding_box::BoundingBox),
}
