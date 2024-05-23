use serde::Deserialize;
use serde::Serialize;
use types::BoundingBox;

pub mod attributes;
pub mod bounding_box;

#[derive(Serialize, Deserialize)]
enum Index {
    BoundingBox(BoundingBox),
    Attributes(attributes::Attributes),
}
