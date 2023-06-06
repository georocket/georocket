/// Chunks are a contiguous array of bytes. They are tagged with their `Source`, allowing this
/// representation to cover UTF8 or ASCII strings, as well as binary formats.
#[derive(Clone)]
pub struct Chunk {
    id: Option<usize>,
    source: Source,
    data: Vec<u8>,
}

impl Chunk {
    pub fn new(id: Option<usize>, source: Source, data: Vec<u8>) -> Self {
        Self { id, source, data }
    }

    pub fn id(&self) -> Option<usize> {
        self.id
    }

    pub fn source(&self) -> Source {
        self.source
    }

    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }
}

/// The `Source` specifies what format a geo data chunk comes from
/// The `Indexer` uses the source, to determine how to process it.
#[derive(Copy, Clone)]
pub enum Source {
    GeoJSON,
}

#[derive(Debug)]
pub struct GeoMetaDataCollection {
    id: Option<usize>,
    inner: Vec<GeoMetaData>,
}

impl GeoMetaDataCollection {
    pub fn new(id: Option<usize>) -> Self {
        Self {
            id,
            inner: Vec::new(),
        }
    }
}

#[derive(Debug)]
enum GeoMetaData {
    //TODO: Add variants such as `BoundingBox` or other meta data that is intended
    // for indexing or other post-processing requirements
}
