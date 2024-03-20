use crate::query::{Comparison, Logic, Primitive, Query, QueryComponent};
use georocket_types::{BoundingBox, Value};

struct QueryTables {
    property: bool,
    bounding_box: bool,
}

impl QueryTables {
    fn merge(&mut self, other: Self) {
        self.property |= other.property;
        self.bounding_box |= other.bounding_box;
    }
    fn new() -> Self {
        Self {
            property: false,
            bounding_box: false,
        }
    }
    fn property() -> Self {
        Self {
            property: true,
            bounding_box: false,
        }
    }
    fn bbox() -> Self {
        Self {
            property: false,
            bounding_box: true,
        }
    }
}

/// Builds a postgis query string from the provided `query`
pub(super) fn build_query(query: Query) -> String {
    let mut header = "SELECT DISTINCT raw_feature FROM georocket.feature f".to_string();
    if query.components.is_empty() {
        header
    } else {
        let mut joins = String::new();
        let (body, query_tables) = queryfy_with_combinator(query.components.as_slice(), "OR");
        if query_tables.property {
            header.push_str(", georocket.property p");
            joins.push_str(" WHERE f.id = p.id AND");
        }
        if query_tables.bounding_box {
            header.push_str(", georocket.bounding_box b");
            // WHERE only needed if a join on properties wasn't specified
            if joins.is_empty() {
                joins.push_str(" WHERE")
            }
            joins.push_str(" f.id = b.id AND")
        }
        format!("{}{} ({})", header, joins, body)
    }
}

/// Creates a query parameter for the `query_component`. Calls the appropriate
fn queryfy(query_component: &QueryComponent) -> (String, QueryTables) {
    match query_component {
        QueryComponent::Primitive(primitive) => queryfy_primative(primitive),
        QueryComponent::Logical(logical) => queryfy_logical(logical),
        QueryComponent::Comparison {
            operator,
            key,
            value,
        } => queryfy_comparison(*operator, key, value),
    }
}

/// Creates a query parameter from each of the `QueryComponents` within the `components` slice.
/// Intersperses the parameters with the specified `combinator`
fn queryfy_with_combinator(
    components: &[QueryComponent],
    combinator: &str,
) -> (String, QueryTables) {
    let mut s = String::new();
    let mut query_tables = QueryTables::new();
    for (component, index) in components.iter().zip(1..) {
        let (qc, loop_query_tables) = queryfy(component);
        query_tables.merge(loop_query_tables);
        s.push_str(&qc);
        if index < components.len() {
            s.push_str(&format!(" {} ", combinator));
        }
    }
    (s, query_tables)
}

fn queryfy_comparison(operator: Comparison, key: &str, value: &Value) -> (String, QueryTables) {
    let s = match value {
        Value::Integer(i) => format!(
            "(p.key = '{}' AND cast(p.value as integer) {} {})",
            key, operator, i
        ),
        Value::Float(f) => format!(
            "(p.key = '{}' AND cast(p.value as float) {} {})",
            key, operator, f
        ),
        Value::String(s) => format!("(p.key = '{}' AND p.value {} '{}')", key, operator, s),
    };
    (s, QueryTables::property())
}

fn queryfy_logical(logical: &Logic) -> (String, QueryTables) {
    match logical {
        Logic::Or(query_components) => {
            let (s, query_tables) = queryfy_with_combinator(query_components, "OR");
            (format!("({})", s), query_tables)
        }
        Logic::And(query_components) => {
            let (s, query_tables) = queryfy_with_combinator(query_components, "AND");
            (format!("({})", s), query_tables)
        }
        Logic::Not(query_component) => {
            let (s, query_tables) = queryfy(query_component);
            (format!("NOT ({})", s), query_tables)
        }
    }
}

fn queryfy_primative(primitive: &Primitive) -> (String, QueryTables) {
    match primitive {
        Primitive::String(s) => (
            format!("(p.key like '%{s}%' OR cast(p.value as text) like '%{s}%')"),
            QueryTables::property(),
        ),
        Primitive::BoundingBox(bbox) => (
            format!("(ST_Intersects(b.bounding_box, {}))", bbox_envelope(bbox)),
            QueryTables::bbox(),
        ),
    }
}

fn bbox_envelope(bbox: &BoundingBox) -> String {
    let (min_x, min_y, max_x, max_y) = bbox.as_tuple();
    format!(
        "ST_MakeEnvelope({}, {}, {}, {}, {})",
        min_x,
        min_y,
        max_x,
        max_y,
        bbox.srid.unwrap_or(0)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use georocket_types::GeoPoint;
    const WSG84_SRID: u32 = 4326;
    const BASIC_BBOX: BoundingBox = BoundingBox {
        srid: Some(WSG84_SRID),
        lower_left: GeoPoint { x: 0.0, y: 0.0 },
        upper_right: GeoPoint { x: 1.0, y: 1.0 },
    };

    #[test]
    fn no_query_argumentes() {
        let query = Query { components: vec![] };
        let query = build_query(query);
        assert_eq!(
            query,
            "SELECT DISTINCT raw_feature FROM georocket.feature f"
        );
    }

    #[test]
    fn query_primitive_bbox() {
        let query = Query {
            components: vec![QueryComponent::Primitive(Primitive::BoundingBox(
                BASIC_BBOX,
            ))],
        };
        let query = build_query(query);
        assert_eq!(
            query,
            "SELECT DISTINCT raw_feature FROM georocket.feature f, georocket.bounding_box b WHERE f.id = b.id AND ((ST_Intersects(b.bounding_box, ST_MakeEnvelope(0, 0, 1, 1, 4326))))"
        );
    }

    #[test]
    fn query_primitive_string() {
        let query = Query {
            components: vec![QueryComponent::Primitive(Primitive::String(
                "test".to_string(),
            ))],
        };
        let query = build_query(query);
        assert_eq!(query,
                   "SELECT DISTINCT raw_feature FROM georocket.feature f, georocket.property p WHERE f.id = p.id AND ((p.key like '%test%' OR cast(p.value as text) like '%test%'))");
    }

    #[test]
    fn query_primitives_string_and_bbox() {
        let query = Query {
            components: vec![
                QueryComponent::Primitive(Primitive::String("test".to_string())),
                QueryComponent::Primitive(Primitive::BoundingBox(BASIC_BBOX)),
            ],
        };
        let query = build_query(query);
        assert_eq!(query,
                   "SELECT DISTINCT raw_feature FROM georocket.feature f, georocket.property p, georocket.bounding_box b WHERE f.id = p.id AND f.id = b.id AND ((p.key like '%test%' OR cast(p.value as text) like '%test%') OR (ST_Intersects(b.bounding_box, ST_MakeEnvelope(0, 0, 1, 1, 4326))))");
    }

    #[test]
    fn query_logical_not() {
        let query = Query {
            components: vec![QueryComponent::Logical(Logic::Not(Box::new(
                QueryComponent::Primitive(Primitive::String("test".to_string())),
            )))],
        };
        let query = build_query(query);
        assert_eq!(query,
                   "SELECT DISTINCT raw_feature FROM georocket.feature f, georocket.property p WHERE f.id = p.id AND (NOT ((p.key like '%test%' OR cast(p.value as text) like '%test%')))");
    }

    #[test]
    fn query_with_combinator() {
        let query = Query {
            components: vec![QueryComponent::Logical(Logic::And(vec![
                QueryComponent::Primitive(Primitive::String("test".to_string())),
                QueryComponent::Primitive(Primitive::BoundingBox(BASIC_BBOX)),
            ]))],
        };
        let query = build_query(query);
        assert_eq!(query,
                   "SELECT DISTINCT raw_feature FROM georocket.feature f, georocket.property p, georocket.bounding_box b WHERE f.id = p.id AND f.id = b.id AND (((p.key like '%test%' OR cast(p.value as text) like '%test%') AND (ST_Intersects(b.bounding_box, ST_MakeEnvelope(0, 0, 1, 1, 4326)))))");
    }
}
