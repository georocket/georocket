package io.georocket.index.postgresql

import io.georocket.constants.ConfigConstants
import io.georocket.index.AbstractIndex
import io.georocket.index.Index
import io.georocket.query.IndexQuery
import io.georocket.storage.ChunkMeta
import io.georocket.util.UniqueID
import io.georocket.util.getAwait
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.await
import io.vertx.sqlclient.Tuple
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

class PostgreSQLIndex private constructor(vertx: Vertx, url: String,
    username: String, password: String) : Index, AbstractIndex() {
  companion object {
    /**
     * Table and column names
     */
    private const val CHUNK_META = "chunkMeta"
    private const val DOCUMENTS = "documents"
    private const val ID = "id"
    private const val DATA = "data"

    suspend fun create(vertx: Vertx, url: String? = null, username: String? = null,
        password: String? = null): PostgreSQLIndex {
      val config = vertx.orCreateContext.config()

      val actualUrl = url ?:
        config.getString(ConfigConstants.INDEX_POSTGRESQL_URL) ?:
        throw IllegalStateException("Missing configuration item `" +
            ConfigConstants.INDEX_POSTGRESQL_URL + "'")

      val actualUsername = username ?:
        config.getString(ConfigConstants.INDEX_POSTGRESQL_USERNAME) ?:
        throw IllegalStateException("Missing configuration item `" +
            ConfigConstants.INDEX_POSTGRESQL_USERNAME + "'")

      val actualPassword = password ?:
        config.getString(ConfigConstants.INDEX_POSTGRESQL_PASSWORD) ?:
        throw IllegalStateException("Missing configuration item `" +
            ConfigConstants.INDEX_POSTGRESQL_PASSWORD + "'")

      return vertx.executeBlocking<PostgreSQLIndex> { p ->
        p.complete(PostgreSQLIndex(vertx, actualUrl, actualUsername, actualPassword))
      }.await()
    }
  }

  private val pgClient = PostgreSQLClient(vertx, url, username, password)
  private val client = pgClient.client

  private suspend fun addOrGetChunkMeta(meta: ChunkMeta): String {
    return addedChunkMetaCache.getAwait(meta) {
      val statement = "WITH new_id AS (" +
            "INSERT INTO $CHUNK_META ($ID, $DATA) VALUES ($1, $2) " +
            "ON CONFLICT DO NOTHING RETURNING $ID" +
          ") SELECT COALESCE(" +
            "(SELECT $ID FROM new_id)," +
            "(SELECT $ID from $CHUNK_META WHERE $DATA=$2)" +
          ")"

      val params = Tuple.of(UniqueID.next(), meta.toJsonObject())

      val r = client.preparedQuery(statement).execute(params).await()
      r.first().getString(0)
    }
  }

  private suspend fun findChunkMeta(id: String): ChunkMeta {
    val r = loadedChunkMetaCache.getAwait(id) {
      val statement = "SELECT $DATA FROM $CHUNK_META WHERE $ID=$1"
      val r = client.preparedQuery(statement).execute(Tuple.of(id)).await()
      r?.first()?.getJsonObject(0)?.let { createChunkMeta(it) } ?:
        throw NoSuchElementException("Could not find chunk metadata with ID `$id' in index")
    }
    return r
  }

  override suspend fun close() {
    client.close()
  }

  override suspend fun addMany(docs: Collection<Index.AddManyParam>) {
    val params = docs.map { d ->
      val chunkMetaId = addOrGetChunkMeta(d.meta)
      val copy = d.doc.copy()
      copy.put(CHUNK_META, chunkMetaId)
      Tuple.of(d.path, copy)
    }
    val statement = "INSERT INTO $DOCUMENTS ($ID, $DATA) VALUES ($1, $2)"
    client.preparedQuery(statement).executeBatch(params).await()
  }

  override suspend fun getDistinctMeta(query: IndexQuery): Flow<ChunkMeta> = flow {
    // TODO add WHERE!!!!
    val statement = "SELECT DISTINCT $DATA->>'$CHUNK_META' FROM $DOCUMENTS"
    pgClient.withConnection { conn ->
      val preparedStatement = conn.prepare(statement).await()
      val transaction = conn.begin().await()
      try {
        val cursor = preparedStatement.cursor()
        try {
          do {
            val rows = cursor.read(50).await()
            emitAll(rows.map { findChunkMeta(it.getString(0)) }.asFlow())
          } while (cursor.hasMore())
        } finally {
          cursor.close()
        }
      } finally {
        transaction.commit()
      }
    }
  }

  override suspend fun getMeta(query: IndexQuery): Flow<Pair<String, ChunkMeta>> = flow {
    // TODO add WHERE!!!!
    val statement = "SELECT $ID, $DATA->>'$CHUNK_META' FROM $DOCUMENTS"
    pgClient.withConnection { conn ->
      val preparedStatement = conn.prepare(statement).await()
      val transaction = conn.begin().await()
      try {
        val cursor = preparedStatement.cursor()
        try {
          do {
            val rows = cursor.read(50).await()
            emitAll(rows.map { it.getString(0) to findChunkMeta(it.getString(1)) }.asFlow())
          } while (cursor.hasMore())
        } finally {
          cursor.close()
        }
      } finally {
        transaction.commit()
      }
    }
  }

  override suspend fun getPaths(query: IndexQuery): Flow<String> {
    TODO("Not yet implemented")
  }

  override suspend fun addTags(query: IndexQuery, tags: Collection<String>) {
    TODO("Not yet implemented")
  }

  override suspend fun removeTags(query: IndexQuery, tags: Collection<String>) {
    TODO("Not yet implemented")
  }

  override suspend fun setProperties(
    query: IndexQuery,
    properties: Map<String, Any>
  ) {
    TODO("Not yet implemented")
  }

  override suspend fun removeProperties(
    query: IndexQuery,
    properties: Collection<String>
  ) {
    TODO("Not yet implemented")
  }

  override suspend fun getPropertyValues(
    query: IndexQuery,
    propertyName: String
  ): Flow<Any?> {
    TODO("Not yet implemented")
  }

  override suspend fun getAttributeValues(
    query: IndexQuery,
    attributeName: String
  ): Flow<Any?> {
    TODO("Not yet implemented")
  }

  override suspend fun delete(query: IndexQuery) {
    TODO("Not yet implemented")
  }

  override suspend fun delete(paths: Collection<String>) {
    TODO("Not yet implemented")
  }

  override suspend fun getCollections(): Flow<String> {
    TODO("Not yet implemented")
  }

  override suspend fun addCollection(name: String) {
    TODO("Not yet implemented")
  }

  override suspend fun existsCollection(name: String): Boolean {
    TODO("Not yet implemented")
  }

  override suspend fun deleteCollection(name: String) {
    TODO("Not yet implemented")
  }
}
