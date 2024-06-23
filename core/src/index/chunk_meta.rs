use ulid::Ulid;

use super::gml::namespaces::Namespaces;

/// Metadata about a chunk
#[derive(PartialEq, Eq, Debug)]
pub struct ChunkMeta {
    /// The chunk's ID
    pub id: Ulid,

    /// Optional XML namespaces
    pub namespaces: Option<Namespaces>,
}

impl ChunkMeta {
    /// Create a new chunk meta object with the given ID and namespaces
    pub fn new(id: Ulid, namespaces: Option<Namespaces>) -> Self {
        Self { id, namespaces }
    }
}
