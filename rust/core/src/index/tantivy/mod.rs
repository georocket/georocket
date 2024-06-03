pub mod json_range_query;
mod query_translator;
pub mod tantivy_index;

use tantivy::schema::Field;
pub use tantivy_index::TantivyIndex;

struct Fields {
    id_field: Field,
    gen_attrs_field: Field,
    all_values_field: Field,
    bbox_min_x_field: Field,
    bbox_min_y_field: Field,
    bbox_min_z_field: Field,
    bbox_max_x_field: Field,
    bbox_max_y_field: Field,
    bbox_max_z_field: Field,
}
