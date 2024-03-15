use crate::query::Query;
use anyhow::Context;
use futures_util::{Stream, StreamExt};
use std::iter::empty;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Error, NoTls};

mod query_build;

type Chunk = Result<String, Error>;

struct GeoJsonFeatureCollection;

impl MergeMeta for GeoJsonFeatureCollection {
    const HEADER: &'static str = r#"{"type":"FeatureCollection","features":["#;
    const FOOTER: &'static str = r#"]}"#;
}

/// Defines constants to be used for creating the header and footer of a specific
/// geo-data format.
pub trait MergeMeta {
    const HEADER: &'static str;
    const FOOTER: &'static str;
}

/// Queries the database specified in the `config` string with the provided `query`.
/// Returns a stream over the retrieved chunks.
pub async fn query(config: &str, query: Query) -> Result<impl Stream<Item = Chunk>, Error> {
    let (client, connection) = tokio_postgres::connect(config, NoTls).await?;
    tokio::spawn(connection);
    let query = query_build::build_query(query);
    let rows = client.query_raw(&query, empty::<&dyn ToSql>()).await?;
    let stream = rows.map(|row| row.map(|r| r.get(0usize)));
    Ok(stream)
}

/// Merges the chunks contained in the `chunk_stream` into the `writer`.
/// The writer is wrapped in an `BufWriter` internally.
pub async fn merge_into<C, W, M>(
    mut chunk_stream: C,
    writer: &mut W,
    merge_meta: M,
) -> anyhow::Result<usize>
where
    C: Stream<Item = Chunk> + Unpin,
    W: AsyncWrite + Unpin,
    M: MergeMeta,
{
    let mut count = 0;
    let mut writer = BufWriter::new(writer);
    writer
        .write(M::HEADER.as_bytes())
        .await
        .context("failed to write into the provided writer")?;
    let mut chunk_stream = chunk_stream;
    while let Some(chunk) = chunk_stream.next().await {
        let chunk = chunk.context("unable to extract chunks from stream")?;
        if count != 0 {
            writer
                .write(",".as_bytes())
                .await
                .context("failed to write into the provided writer")?;
        }
        writer
            .write(chunk.as_bytes())
            .await
            .context("failed to write into the provided writer")?;
        count += 1;
    }
    writer
        .write(M::FOOTER.as_bytes())
        .await
        .context("failed to write into the provided writer")?;
    writer
        .flush()
        .await
        .context("failed to write into the provided writer")?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::{once, repeat};
    use uuid::Uuid;

    fn chunk_from_str(s: &str) -> Chunk {
        Ok(s.to_string())
    }

    /// attempts to merge an empty stream of chunks into an in memory buffer
    #[tokio::test]
    async fn test_merge_empty() {
        let chunks: Vec<Chunk> = vec![];
        let mut buffer: Vec<u8> = vec![];
        let count = merge_into(
            futures_util::stream::iter(chunks.into_iter()),
            &mut buffer,
            GeoJsonFeatureCollection,
        )
        .await
        .unwrap();
        let written = String::from_utf8(buffer).expect("written bytes should be valid UTF8");
        assert_eq!(written, r#"{"type":"FeatureCollection","features":[]}"#);
        assert_eq!(count, 0);
    }

    /// Attempts to merge an empty stream of chunks into a temporary file
    /// Mainly to test if actual IO works as expected.
    #[tokio::test]
    async fn test_merge_empty_file() {
        let chunks: Vec<Chunk> = vec![];
        let chunks = futures_util::stream::iter(chunks);
        let mut tmp = std::env::temp_dir();
        tmp.push(Uuid::new_v4().to_string());
        // in scope, to make sure `dest` is properly closed.
        let count = {
            let mut dest = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&tmp)
                .await
                .expect("unable to create file in temp directory: {}");
            merge_into(chunks, &mut dest, GeoJsonFeatureCollection)
                .await
                .unwrap()
        };
        let written = tokio::fs::read_to_string(&tmp).await.unwrap();
        assert_eq!(written, r#"{"type":"FeatureCollection","features":[]}"#);
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_merge_one() {
        let chunk = r#"{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [ 102.0, 0.5 ] }, "properties": { "prop0": "value0" } }"#;
        let control = format!(r#"{{"type":"FeatureCollection","features":[{}]}}"#, chunk);
        let chunks: Vec<Chunk> = once(chunk).map(chunk_from_str).collect();
        let chunks = futures_util::stream::iter(chunks);
        let mut dest = vec![];
        let count = merge_into(chunks, &mut dest, GeoJsonFeatureCollection)
            .await
            .unwrap();
        let written = String::from_utf8(dest).expect("written bytes should be valid UTF8");
        assert_eq!(written, control);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_merge_multiple() {
        let chunk = r#"{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [ 102.0, 0.5 ] }, "properties": { "prop0": "value0" } }"#;
        let control = format!(
            r#"{{"type":"FeatureCollection","features":[{},{},{}]}}"#,
            chunk, chunk, chunk
        );
        let chunks: Vec<Chunk> = repeat(chunk).take(3).map(chunk_from_str).collect();
        let chunks = futures_util::stream::iter(chunks);
        let mut dest = vec![];
        let count = merge_into(chunks, &mut dest, GeoJsonFeatureCollection)
            .await
            .unwrap();
        let written = String::from_utf8(dest).expect("written bytes should be valid UTF8");
        assert_eq!(written, control);
        assert_eq!(count, 3);
    }
}
