pub mod json_range_query;
pub mod tantivy_index;
mod translate_query;

use tantivy::schema::Field;
pub use tantivy_index::TantivyIndex;

struct Fields {
    id_field: Field,
    gen_attrs_field: Field,
    all_values_field: Field,
}
