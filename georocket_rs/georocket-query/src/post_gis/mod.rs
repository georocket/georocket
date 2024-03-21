use crate::query::Query;
use anyhow::Context;
use futures_util::{Stream, StreamExt};
use std::iter::empty;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Error, NoTls};

mod query_build;

pub(crate) type Chunk = Result<String, Error>;

/// Queries the database specified in the `config` string with the provided `query`, creating
/// a connection to the database and returning a stream over the retrieved chunks.
pub async fn query(config: &str, query: Query) -> Result<impl Stream<Item = Chunk>, Error> {
    let (client, connection) = tokio_postgres::connect(config, NoTls).await?;
    tokio::spawn(connection);
    query_client(&client, query).await
}

/// Queries the database the client is connected to, returning a stream over the retrieved chunks.
pub async fn query_client(
    client: &Client,
    query: Query,
) -> Result<impl Stream<Item = Chunk>, Error> {
    let query = query_build::build_query(query);
    dbg!(&query);
    let rows = client.query_raw(&query, empty::<&dyn ToSql>()).await?;
    let stream = rows.map(|row| row.map(|r| r.get(0usize)));
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Comparison, Logic, QueryComponent};
    use futures_util::TryStreamExt;
    use geo_testcontainer::postgis::PostGIS as PostGISContainer;
    use geo_testcontainer::testcontainers::clients::Cli;
    use geo_testcontainer::testcontainers::Container;
    use georocket_migrations::postgis::migrations as postgis_migrations;
    use once_cell::sync::Lazy;
    use std::path::Path;

    static TEST_CONT_CLI: Lazy<Cli> = Lazy::new(|| Cli::default());

    async fn setup(
        test_features: impl AsRef<Path>,
    ) -> (Container<'static, PostGISContainer>, Client) {
        let image = PostGISContainer::new().with_host_auth();
        let container = TEST_CONT_CLI.run(image);
        let port = container.get_host_port_ipv4(5432);
        let config = format!("host=localhost port={} user=postgres", port);
        let (mut client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        tokio::spawn(connection);
        postgis_migrations::runner()
            .run_async(&mut client)
            .await
            .unwrap();
        let sql = tokio::fs::read_to_string(test_features).await.unwrap();
        client.simple_query(&sql).await.unwrap();
        (container, client)
    }

    #[tokio::test]
    async fn simple_features_no_query_specifier() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let query = Query { components: vec![] };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn simple_features_select_all_bbox() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let bounding_box =
            georocket_types::BoundingBox::from_tuple((0.0, 0.0, 10.0, 10.0, Some(4326)));
        let query = Query {
            components: vec![bounding_box.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn simple_feature_select_not_all_bbox() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let bounding_box =
            georocket_types::BoundingBox::from_tuple((0.0, 0.0, 10.0, 10.0, Some(4326)));
        let not_bounding_box = Logic::not(bounding_box);
        let query = Query {
            components: vec![not_bounding_box.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 0);
    }

    #[tokio::test]
    async fn simple_feature_select_some_bbox() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let bounding_box =
            georocket_types::BoundingBox::from_tuple((0.0, 0.0, 5.0, 8.0, Some(4326)));
        let query = Query {
            components: vec![bounding_box.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks.contains(&"feature_a".to_string()));
        assert!(chunks.contains(&"feature_b".to_string()));
    }

    #[tokio::test]
    async fn simple_feature_select_not_some_bbox() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let bounding_box =
            georocket_types::BoundingBox::from_tuple((0.0, 0.0, 5.0, 8.0, Some(4326)));
        let not_bounding_box = Logic::not(bounding_box);
        let query = Query {
            components: vec![not_bounding_box.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(chunks.contains(&"feature_c".to_string()));
    }

    #[tokio::test]
    async fn simple_feature_select_and_bbox() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let bounding_box_a =
            georocket_types::BoundingBox::from_tuple((0.0, 0.0, 9.0, 5.0, Some(4326)));
        let bounding_box_b =
            georocket_types::BoundingBox::from_tuple((4.0, 3.0, 9.9, 9.0, Some(4326)));
        let and_bounding_boxes = Logic::And(vec![bounding_box_a.into(), bounding_box_b.into()]);
        let query = Query {
            components: vec![and_bounding_boxes.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(chunks.contains(&"feature_c".to_string()));
    }

    #[tokio::test]
    async fn simple_feature_select_or_bbox() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let bounding_box_a =
            georocket_types::BoundingBox::from_tuple((0.0, 0.0, 9.0, 5.0, Some(4326)));
        let bounding_box_b =
            georocket_types::BoundingBox::from_tuple((4.0, 3.0, 9.9, 9.0, Some(4326)));
        let and_bounding_boxes = Logic::Or(vec![bounding_box_a.into(), bounding_box_b.into()]);
        let query = Query {
            components: vec![and_bounding_boxes.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn simple_feature_select_primitive_string() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let s = "key".to_string();
        let query = Query {
            components: vec![s.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn simple_feature_select_primitive_number() {
        let (_container, client) = setup("test_files/simple_features_postgis.sql").await;
        let s = "1".to_string();
        let query = Query {
            components: vec![s.into()],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn simple_comparison_equal() {
        let (_container, client) = setup("test_files/simple_comparison_postgis.sql").await;
        let qc: QueryComponent = (Comparison::Eq, "key", 2).into();
        let query = Query {
            components: vec![qc],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(chunks.contains(&"feature_b".to_string()));
    }

    #[tokio::test]
    async fn simple_comparison_less_than() {
        let (_container, client) = setup("test_files/simple_comparison_postgis.sql").await;
        let qc: QueryComponent = (Comparison::Lt, "key", 2).into();
        let query = Query {
            components: vec![qc],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(chunks.contains(&"feature_a".to_string()));
    }

    #[tokio::test]
    async fn simple_comparison_less_than_equal() {
        let (_container, client) = setup("test_files/simple_comparison_postgis.sql").await;
        let qc: QueryComponent = (Comparison::Lte, "key", 2).into();
        let query = Query {
            components: vec![qc],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks.contains(&"feature_a".to_string()));
        assert!(chunks.contains(&"feature_b".to_string()));
    }

    #[tokio::test]
    async fn simple_comparison_not_equal() {
        let (_container, client) = setup("test_files/simple_comparison_postgis.sql").await;
        let qc: QueryComponent = (Comparison::Neq, "key", 2).into();
        let query = Query {
            components: vec![qc],
        };
        let chunks: Vec<String> = query_client(&client, query)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks.contains(&"feature_a".to_string()));
        assert!(chunks.contains(&"feature_c".to_string()));
    }
}
