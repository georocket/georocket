use super::index_map::IdIndexMap;
use super::Store;
use crate::types::{Index, IndexElement, RawChunk};
use futures::future::BoxFuture;
use indexing::attributes::{Attributes, Value};
use indexing::bounding_box::{BoundingBox, GeoPoint};
use std::str::from_utf8;
use tokio::sync::mpsc;
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
    raw_rec: mpsc::Receiver<RawChunk>,
    index_rec: mpsc::Receiver<Index>,
    client: Client,
    id_index_map: IdIndexMap,
}

impl PostGISStore {
    /// Creates a new `PostGISStore` with a connection based on the `config`.
    /// The `PostGISStore` receives chunks and indexes from `raw_rec` and `index_rec` channels and stores
    /// them in the specified database.
    pub async fn new(
        mut client: Client,
        raw_rec: mpsc::Receiver<RawChunk>,
        index_rec: mpsc::Receiver<Index>,
    ) -> anyhow::Result<Self> {
        migration::migrations::runner()
            .run_async(&mut client)
            .await?;
        Ok(Self {
            raw_rec,
            index_rec,
            client,
            id_index_map: IdIndexMap::new(),
        })
    }

    pub fn construct_with_client(
        client: Client,
    ) -> impl FnOnce(
        mpsc::Receiver<RawChunk>,
        mpsc::Receiver<Index>,
    ) -> BoxFuture<'static, anyhow::Result<Box<dyn Store + Send>>> {
        |raw_rec, idx_rex| {
            Box::pin(async move {
                let store = PostGISStore::new(client, raw_rec, idx_rex).await?;
                Ok(Box::new(store) as Box<dyn Store + Send>)
            })
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<usize> {
        let mut num_indexes = 0;
        let mut num_chunks = 0;
        loop {
            tokio::select! {
                Some(raw) = self.raw_rec.recv() => {
                    self.handle_raw(raw).await?;
                    num_chunks += 1;
                }
                Some(index) = self.index_rec.recv() => {
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
        let geometry = match bbox {
            BoundingBox::Point(point) => {
                let GeoPoint { x, y } = point;
                format!(r#"{{"type":"Point","coordinates":[{},{}]}}"#, x, y)
            }
            BoundingBox::Box([p0, p1, p2, p3, p4]) => {
                format!(
                    r#"{{"type:"Polygon,"coordinates":[[[{}, {}],[{}, {}],[{}, {}],[{}, {}],[{}, {}]]]"}}"#,
                    p0.x, p0.y, p1.x, p1.y, p2.x, p2.y, p3.x, p3.y, p4.x, p4.y
                )
            }
        };
        self.client
            .execute(
                "\
                    INSERT INTO georocket.bounding_box (id, bounding_box)\
                    VALUES ($1, ST_GeomFromGeoJSON($2))
                    ",
                &[&uuid, &geometry],
            )
            .await?;
        Ok(())
    }

    async fn handle_attributes(&self, attributes: Attributes, uuid: Uuid) -> anyhow::Result<()> {
        for (key, value) in attributes {
            let value = match value {
                Value::String(val) => val,
                Value::Double(d_val) => d_val.to_string(),
                Value::Integer(i_val) => i_val.to_string(),
            };
            self.client
                .execute(
                    "\
                    INSERT INTO georocket.property (id, key, value)\
                    VALUES ($1, $2, $3)
                ",
                    &[&uuid, &key, &value],
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::importer::GeoDataImporter;
    use crate::indexer::MainIndexer;
    use crate::input::geo_json_splitter::GeoJsonSplitter;
    use crate::input::SplitterChannels;
    use geo_testcontainer::postgis::PostGIS;
    use geo_testcontainer::testcontainers::{clients, Image};
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn postgis_store_main_test() {
        let docker = clients::Cli::default();
        let postgis_image = PostGIS::default().with_host_auth();
        let node = docker.run(postgis_image);
        let port = node.get_host_port_ipv4(5432);
        let config_string = format!("host=localhost user=postgres port={}", port);
        assert_database_setup(&config_string).await;
        assert_writing_data(&config_string).await;
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
        let _store = PostGISStore::new(store_client, raw_rec, idx_rec)
            .await
            .unwrap();

        // select select all table names in the georocket schema
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

        // check that the property table has the correct columns and types
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
        assert_eq!(rows.len(), 3);
        assert!(
            rows.contains(&("id".into(), "uuid".into()))
                && rows.contains(&("key".into(), "text".into()))
                && rows.contains(&("value".into(), "text".into()))
        );

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
    async fn assert_writing_data(config: &str) {
        write_to_db("test_files/simple_feature_01.json", config).await;
        let (test_client, test_connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
        tokio::spawn(test_connection);

        let feature = test_client
            .query("SELECT * from georocket.feature", &[])
            .await
            .unwrap();
        // dbg!(feature.len());
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
            .query("SELECT key, value from georocket.property", &[])
            .await
            .unwrap();
        assert_eq!(properties.len(), 1);
        let (key, value): (String, String) = (properties[0].get(0), properties[0].get(1));
        assert_eq!(key, "name");
        assert_eq!(value, "Dinagat Islands");
    }

    async fn write_to_db(file: &str, config: &str) {
        let (store_client, store_connection) =
            tokio_postgres::connect(config, NoTls).await.unwrap();
        tokio::spawn(store_connection);
        let (splitter_channels, chunk_rec, raw_rec) =
            SplitterChannels::new_with_channels(1024, 1024);
        let splitter = GeoJsonSplitter::new(
            tokio::fs::File::open(file).await.unwrap(),
            splitter_channels,
        );
        let (indexer, index_rec) = MainIndexer::new_with_index_receiver(1024, chunk_rec);
        let store = PostGISStore::new(store_client, raw_rec, index_rec)
            .await
            .unwrap();
        let geo_data_importer = GeoDataImporter::new(Box::new(splitter), Box::new(store), indexer);
        geo_data_importer.run().await.unwrap();
    }
}
