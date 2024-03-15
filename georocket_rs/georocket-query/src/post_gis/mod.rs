use crate::query::Query;
use anyhow::Context;
use futures_util::{Stream, StreamExt};
use std::iter::empty;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Error, NoTls};

mod query_build;

pub(crate) type Chunk = Result<String, Error>;

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
