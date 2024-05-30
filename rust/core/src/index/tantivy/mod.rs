pub mod json_range_query;
mod query_translator;
pub mod tantivy_index;

use tantivy::schema::Field;
pub use tantivy_index::TantivyIndex;

struct Fields {
    id_field: Field,
    gen_attrs_field: Field,
    all_values_field: Field,
}
