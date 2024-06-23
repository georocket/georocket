use ulid::Ulid;

use super::gml::root_element::RootElement;

/// Metadata about a chunk
#[derive(PartialEq, Eq, Debug)]
pub struct ChunkMeta {
    /// The chunk's ID
    pub id: Ulid,

    /// The chunk's XML root element
    pub root_element: RootElement,
}

impl ChunkMeta {
    /// Create a new chunk meta object with the given ID and namespaces
    pub fn new(id: Ulid, root_element: RootElement) -> Self {
        Self { id, root_element }
    }
}
