#[derive(Clone, Debug)]
pub(crate) struct Chunk<E, H> {
    index_meta: IndexMeta<H>,
    data: Vec<E>,
}

#[derive(Clone, Debug)]
struct IndexMeta<H> {
    filename: String,
    header: H,
}

impl<H> IndexMeta<H>
where
    H: Header,
{
    fn source(&self) -> Source {
        self.header.source()
    }
}

trait Header {
    fn source(&self) -> Source;
}

impl<E, H> Chunk<E, H> {
    pub fn new(index_meta: IndexMeta<H>) -> Self {
        Self {
            index_meta,
            data: vec![],
        }
    }

    pub fn push(&mut self, data: E) {
        self.data.push(data);
    }

    pub fn index_meta(&self) -> &IndexMeta<H> {
        &self.index_meta
    }

    pub fn data(&self) -> &[E] {
        &self.data
    }
}

/// The `Source` specifies what format a geo data chunk comes from
/// The `Indexer` uses the source, to determine how to process it.
#[derive(Copy, Clone)]
pub enum Source {
    GeoJSON,
}
