pub mod bounding_box_query;
pub mod json_range_query;
mod query_translator;
pub mod tantivy_index;

#[cfg(test)]
mod iter_scorer;

use tantivy::schema::Field;
pub use tantivy_index::TantivyIndex;

struct Fields {
    id: Field,
    root_element: Field,
    gen_attrs: Field,
    all_values: Field,
    bbox_min_x: Field,
    bbox_min_y: Field,
    bbox_max_x: Field,
    bbox_max_y: Field,
    bbox_terms: Field,
}
