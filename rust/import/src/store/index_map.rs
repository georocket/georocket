use std::collections::HashMap;
use uuid::Uuid;

type Index = Uuid;
type Inner = HashMap<usize, Index>;

pub(crate) struct IdIndexMap(pub(crate) Inner);

impl IdIndexMap {
    pub fn new() -> Self {
        IdIndexMap(HashMap::new())
    }
    pub fn get_or_create_index(&mut self, id: usize) -> Index {
        let index = *self.0.entry(id).or_insert_with(|| Index::new_v4());
        index
    }
}

impl From<HashMap<usize, Index>> for IdIndexMap {
    fn from(value: HashMap<usize, Index>) -> Self {
        IdIndexMap(value)
    }
}
