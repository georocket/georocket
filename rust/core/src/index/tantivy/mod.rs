pub mod tantivy_index;
mod translate_query;

use tantivy::schema::Field;
pub use tantivy_index::TantivyIndex;

struct Fields {
    id_field: Field,
    gen_attrs_field: Field,
}
