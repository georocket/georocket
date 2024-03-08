use super::index_map::IdIndexMap;
use crate::store::channels::StoreChannels;
use crate::types::{Index, IndexElement, RawChunk};
use georocket_types::{BoundingBox, Value};
use indexing::attributes::Attributes;
use std::str::from_utf8;
use tokio_postgres;
use tokio_postgres::Client;
use uuid::Uuid;

mod migration {
    refinery::embed_migrations!();
}

/// The `PostGISStore` receives `RawChunk`s from a Splitter and `Index`es from
/// the `MainIndexer`. It stores these in a [PostGIS](https://postgis.net/) database.
/// A UUID is generated, to map the raw features to their respective indexes.
pub struct PostGISStore {
    store_channels: StoreChannels,
    client: Client,
    id_index_map: IdIndexMap,
}

impl PostGISStore {
    /// Creates a new `PostGISStore` with a connection based on the `config`.
    /// The `PostGISStore` receives chunks and indexes from `raw_rec` and `index_rec` channels and stores
    /// them in the specified database.
    pub async fn new(mut client: Client, store_channels: StoreChannels) -> anyhow::Result<Self> {
        migration::migrations::runner()
            .run_async(&mut client)
            .await?;
        return Ok(Self {
            store_channels,
            client,
            id_index_map: IdIndexMap::new(),
        });
    }

    pub async fn run(&mut self) -> anyhow::Result<usize> {
        let mut num_indexes = 0;
        let mut num_chunks = 0;
        loop {
            tokio::select! {
                Some(raw) = self.store_channels.raw_rec.recv() => {
                    self.handle_raw(raw).await?;
                    num_chunks += 1;
                }
                Some(index) = self.store_channels.index_rec.recv() => {
                    self.handle_index(index).await?;
                    num_indexes += 1;
                }
                else => {
                    break;
                }
            }
        }
        if num_chunks == num_indexes {
            Ok(num_indexes)
        } else {
            Err(anyhow::anyhow!(
                "Number of chunks ({}) written does not match number of indexes ({}) written",
                num_chunks,
                num_indexes
            ))
        }
    }

    /// Stores the `RawChunk` in the database.
    /// Creates a new `index_map::Index` or uses the existing index, if there is already
    /// one present.
    pub async fn handle_raw(&mut self, raw: RawChunk) -> anyhow::Result<()> {
        let RawChunk { id, raw } = raw;
        let feature = from_utf8(raw.as_slice()).expect("raw chunk not valid utf-8");
        let index = self.id_index_map.get_or_create_index(id);
        self.client
            .execute(
                "\
        INSERT INTO georocket.feature (id, raw_feature)\
        VALUES ($1, $2)",
                &[&index, &feature],
            )
            .await?;
        Ok(())
    }

    /// Stores the `IndexElement`s contained in the `Index` in the database.
    /// Delegates to `handle_bounding_box` and `handle_attributes` for `IndexElement::BoundingBox`
    /// and `IndexElement::Attributes` respectively.
    /// Creates a new `index_map::Index` or uses the existing index, if there is already
    /// one present.
    async fn handle_index(&mut self, index: Index) -> anyhow::Result<()> {
        let Index { id, index_elements } = index;
        let uuid = self.id_index_map.get_or_create_index(id);
        for ie in index_elements {
            match ie {
                IndexElement::BoundingBoxIndex(bbox) => {
                    self.handle_bounding_box(bbox, uuid).await?
                }
                IndexElement::Attributes(attributes) => {
                    self.handle_attributes(attributes, uuid).await?
                }
            }
        }
        Ok(())
    }

    async fn handle_bounding_box(&self, bbox: BoundingBox, uuid: Uuid) -> anyhow::Result<()> {
        let BoundingBox {
            srid,
            lower_left,
            upper_right,
        } = bbox;
        if lower_left == upper_right {
            self.client
                .execute(
                    "\
                INSERT INTO georocket.bounding_box (id, bounding_box)\
                VALUES ($1, ST_Point($2, $3, $4))",
                    &[
                        &uuid,
                        &lower_left.x,
                        &lower_left.y,
                        &(srid.unwrap_or(0) as i32),
                    ],
                )
                .await?;
        } else {
            self.client
                .execute(
                    "\
                    INSERT INTO georocket.bounding_box (id, bounding_box)\
                    VALUES ($1, ST_MakeEnvelope($2, $3, $4, $5, $6))
                    ",
                    &[
                        &uuid,
                        &lower_left.x,
                        &lower_left.y,
                        &upper_right.x,
                        &upper_right.y,
                        &(srid.unwrap_or(0) as i32),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    async fn handle_attributes(&self, attributes: Attributes, uuid: Uuid) -> anyhow::Result<()> {
        for (key, value) in attributes {
            match value {
                Value::String(val) => {
                    self.client
                        .execute(
                            "\
                    INSERT INTO georocket.property (id, key, value_s)\
                    VALUES ($1, $2, $3)",
                            &[&uuid, &key, &val],
                        )
                        .await
                }
                Value::Float(d_val) => {
                    self.client
                        .execute(
                            "\
                    INSERT INTO georocket.property (id, key, value_f)\
                    VALUES ($1, $2, $3)",
                            &[&uuid, &key, &d_val],
                        )
                        .await
                }
                Value::Integer(i_val) => {
                    self.client
                        .execute(
                            "\
                    INSERT INTO georocket.property (id, key, value_i)\
                    VALUES ($1, $2, $3)",
                            &[&uuid, &key, &i_val],
                        )
                        .await
                }
            }?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::ImporterBuilder;
    use crate::{SourceType, StoreType};
    use geo_testcontainer::postgis::PostGIS;
    use geo_testcontainer::testcontainers::clients;
    use std::collections::HashMap;
    use tokio::sync::mpsc;
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn postgis_store_main_test() {
        let docker = clients::Cli::default();
        let postgis_image = PostGIS::default().with_host_auth();
        let node = docker.run(postgis_image);
        let port = node.get_host_port_ipv4(5432);
        let config_string = format!("host=localhost user=postgres port={}", port);
        assert_database_setup(&config_string).await;
        assert_writing_simple_feature(&config_string).await;
        assert_writing_simple_collection(&config_string).await;
    }

    /// Checks if the database has been set up correctly
    /// Opens a new connection to the specified database.
    async fn assert_database_setup(config: &str) {
        let (store_client, store_connection) =
            tokio_postgres::connect(config, NoTls).await.unwrap();
        let (test_client, test_connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
        tokio::spawn(test_connection);
        tokio::spawn(store_connection);
        let (_, raw_rec) = mpsc::channel(1024);
        let (_, idx_rec) = mpsc::channel(1024);
        let store_channels = StoreChannels::new(raw_rec, idx_rec);
        let _store = PostGISStore::new(store_client, store_channels)
            .await
            .unwrap();

        // select all table names in the georocket schema
        let rows = test_client
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'georocket'",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                let table_name: String = row.get(0);
                table_name
            })
            .collect::<Vec<_>>();

        // check that all the tables are present
        assert_eq!(rows.len(), 3);
        assert!(
            rows.contains(&"feature".to_string())
                && rows.contains(&"bounding_box".to_string())
                && rows.contains(&"property".to_string())
        );

        let column_query = test_client
            .prepare(
                "\
        SELECT column_name, data_type FROM information_schema.columns \
        WHERE table_schema = 'georocket' \
            AND \
               table_name = $1::TEXT",
            )
            .await
            .expect("failed to prepare column query");

        // check that the tables have the correct columns and types
        let rows = test_client
            .query(&column_query, &[&"feature"])
            .await
            .expect("failed to query feature table")
            .into_iter()
            .map(|row| {
                let column_name: String = row.get(0);
                let data_type: String = row.get(1);
                (column_name, data_type)
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.contains(&("id".into(), "uuid".into()))
                && rows.contains(&("raw_feature".into(), "text".into()))
        );

        // check that the property table has the correct number of columns
        let rows = test_client
            .query(&column_query, &[&"property"])
            .await
            .expect("failed to query property table")
            .into_iter()
            .map(|row| {
                let column_name: String = row.get(0);
                let data_type: String = row.get(1);
                (column_name, data_type)
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 5);

        // check that the bounding_box table has the correct columns and types
        let rows = test_client
            .query(&column_query, &[&"bounding_box"])
            .await
            .expect("failed to query bounding_box tax`ble")
            .into_iter()
            .map(|row| {
                let column_name: String = row.get(0);
                let data_type: String = row.get(1);
                (column_name, data_type)
            })
            .collect::<Vec<_>>();
        dbg!(&rows);
        assert_eq!(rows.len(), 2);
        assert!(
            rows.contains(&("id".into(), "uuid".into()))
                && rows.contains(&("bounding_box".into(), "USER-DEFINED".into()))
        );

        // check that the user defined type is actually a geometry
        let bounding_box_type = test_client
            .query_one(
                "SELECT udt_name from information_schema.columns \
                where table_schema = 'georocket' \
                AND \
                      table_name = 'bounding_box' \
                AND \
                       column_name = 'bounding_box'",
                &[],
            )
            .await
            .expect("failed to query bounding_box table");
        assert_eq!(bounding_box_type.get::<_, &str>(0), "geometry");
    }

    /// Checks if data can be written to the database and if the
    /// data is written correctly.
    async fn assert_writing_simple_feature(config: &str) {
        write_to_db("test_files/simple_feature_01.json", config).await;
        let (test_client, test_connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
        tokio::spawn(test_connection);

        let feature = test_client
            .query("SELECT * from georocket.feature", &[])
            .await
            .unwrap();
        assert_eq!(feature.len(), 1);
        let feature: String = feature[0].get(1);
        assert_eq!(
            r#"{
  "type": "Feature",
  "geometry": {
    "type": "Point",
    "coordinates": [125.6, 10.1]
  },
  "properties": {
    "name": "Dinagat Islands"
  }
}"#,
            &feature
        );

        let bounding_box = test_client
            .query(
                "SELECT st_asgeojson(bounding_box) from georocket.bounding_box",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(bounding_box.len(), 1);
        let bounding_box: String = bounding_box[0].get(0);
        assert_eq!(
            r#"{"type":"Point","coordinates":[125.6,10.1]}"#,
            bounding_box
        );

        let properties = test_client
            .query("SELECT key, value_s from georocket.property", &[])
            .await
            .unwrap();
        assert_eq!(properties.len(), 1);
        let (key, value): (String, String) = (properties[0].get(0), properties[0].get(1));
        assert_eq!(key, "name");
        assert_eq!(value, "Dinagat Islands");
        test_client
            .execute("TRUNCATE TABLE georocket.feature", &[])
            .await
            .unwrap();
        test_client
            .execute("TRUNCATE TABLE georocket.property", &[])
            .await
            .unwrap();
        test_client
            .execute("TRUNCATE TABLE georocket.bounding_box", &[])
            .await
            .unwrap();
    }

    async fn assert_writing_simple_collection(config: &str) {
        let feature1 = r#"{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [ 102.0, 0.5 ] }, "properties": { "identity": 1, "prop0": "value0" } }"#;
        let bbox_feature1 = r#"{"type":"Point","coordinates":[102,0.5]}"#;
        let feature1_properties = &[("identity", 1.into()), ("prop0", "value0".into())];
        let feature2 = r#"{ "type": "Feature", "geometry": { "type": "LineString", "coordinates": [ [ 102.0, 0.0 ], [ 103.0, 1.0 ], [ 104.0, 0.0 ], [ 105.0, 1.0 ] ] }, "properties": { "identity": 2, "prop0": "value0", "prop1": 0.0 } }"#;
        let bbox_feature2 =
            r#"{"type":"Polygon","coordinates":[[[102,0],[102,1],[105,1],[105,0],[102,0]]]}"#;
        let feature2_properties = &[
            ("identity", 2.into()),
            ("prop0", "value0".into()),
            ("prop1", 0.0.into()),
        ];
        let feature3 = r#"{ "type": "Feature", "geometry": { "type": "Polygon", "coordinates": [ [ [ 100.0, 0.0 ], [ 101.0, 0.0 ], [ 101.0, 1.0 ], [ 100.0, 1.0 ], [ 100.0, 0.0 ] ] ] }, "properties": { "identity": 3, "prop0": "value0", "prop1": { "this": "that" } } }"#;
        let bbox_feature3 =
            r#"{"type":"Polygon","coordinates":[[[100,0],[100,1],[101,1],[101,0],[100,0]]]}"#;
        let feature3_properties = &[
            ("identity", 3.into()),
            ("prop0", "value0".into()),
            ("prop1.this", "that".into()),
        ];
        write_to_db("test_files/simple_collection_02.json", config).await;
        let (test_client, test_connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
        tokio::spawn(test_connection);
        let identities = test_client
            .query(
                "SELECT id, key, value_i FROM georocket.property WHERE key = 'identity'",
                &[],
            )
            .await
            .unwrap();

        for identity in identities {
            let id: Uuid = identity.get(0);
            let value: i64 = identity.get(2);
            match value {
                1 => {
                    assert_match(
                        &test_client,
                        id,
                        feature1,
                        bbox_feature1,
                        feature1_properties,
                    )
                    .await
                }
                2 => {
                    assert_match(
                        &test_client,
                        id,
                        feature2,
                        bbox_feature2,
                        feature2_properties,
                    )
                    .await
                }
                3 => {
                    assert_match(
                        &test_client,
                        id,
                        feature3,
                        bbox_feature3,
                        feature3_properties,
                    )
                    .await
                }
                _ => panic!("unexpected identity value: {}", value),
            }
        }

        /// checks if the feature identified by `id` matches the control feature, bbox and properties
        async fn assert_match(
            client: &Client,
            id: Uuid,
            control_feature: &str,
            control_bbox: &str,
            control_properties: &[(&str, Value)],
        ) {
            let feature: String = client
                .query_one(
                    "SELECT raw_feature from georocket.feature where id = $1",
                    &[&id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(feature, control_feature);
            let bbox: String = client
                .query_one(
                    "SELECT st_asgeojson(bounding_box) from georocket.bounding_box where id = $1",
                    &[&id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(bbox, control_bbox);
            let properties = client
                .query(
                    "SELECT key, value_f, value_i, value_s from georocket.property where id = $1",
                    &[&id],
                )
                .await
                .unwrap()
                .into_iter()
                .map(|row| {
                    let key: String = row.get(0);
                    let value_f: Option<f64> = row.get(1);
                    let value_i: Option<i64> = row.get(2);
                    let value_s: Option<String> = row.get(3);
                    (key, (value_f, value_i, value_s))
                })
                .collect::<HashMap<_, _>>();
            assert_eq!(properties.len(), control_properties.len());
            dbg!(&properties);
            for (key, value) in control_properties {
                let property = dbg!(properties.get(dbg!(*key)));
                match value {
                    Value::String(s) => {
                        assert_eq!(property.unwrap().2.as_ref().unwrap(), s);
                    }
                    Value::Float(f) => {
                        assert_eq!(property.unwrap().0.unwrap(), *f);
                    }
                    Value::Integer(i) => {
                        assert_eq!(property.unwrap().1.unwrap(), *i);
                    }
                }
            }
        }
    }
    async fn write_to_db(file: &str, config: &str) {
        let importer = ImporterBuilder::new()
            .with_store_type(StoreType::PostGIS)
            .with_store_config(config.to_string())
            .with_source_type(SourceType::GeoJsonFile)
            .with_source_config(file.to_string())
            .build()
            .await
            .unwrap();
        importer.run().await.unwrap();
    }
}
