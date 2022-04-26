package io.georocket.index.postgresql

import io.georocket.constants.ConfigConstants
import io.georocket.index.AbstractIndex
import io.georocket.index.DatabaseIndex
import io.georocket.index.Index
import io.georocket.index.normalizeLayer
import io.georocket.query.Compare
import io.georocket.query.IndexQuery
import io.georocket.query.QueryPart
import io.georocket.query.StartsWith
import io.georocket.storage.ChunkMeta
import io.georocket.util.UniqueID
import io.georocket.util.getAwait
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.Tuple
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory

class PostgreSQLIndex private constructor(
  vertx: Vertx, url: String,
  username: String, password: String
) : Index, AbstractIndex() {
  companion object {

    private val log = LoggerFactory.getLogger(PostgreSQLIndex::class.java)

    /**
     * Table and column names
     */
    private const val CHUNK_META = "chunkMeta"
    private const val DOCUMENTS = "documents"
    private const val ID = "id"
    private const val DATA = "data"
    private const val TAGS = "tags"
    private const val PROPS = "props"
    private const val GENATTRS = "genAttrs"
    private const val KEY = "key"
    private const val VALUE = "value"
    private const val NAME = "name"
    private const val LAYER = "layer"
    private const val IDX_PREFIX = "georocket_"

    suspend fun create(
      vertx: Vertx, url: String? = null, username: String? = null,
      password: String? = null
    ): PostgreSQLIndex {
      val config = vertx.orCreateContext.config()

      val actualUrl = url ?: config.getString(ConfigConstants.INDEX_POSTGRESQL_URL) ?: throw IllegalStateException(
        "Missing configuration item `" +
          ConfigConstants.INDEX_POSTGRESQL_URL + "'"
      )

      val actualUsername =
        username ?: config.getString(ConfigConstants.INDEX_POSTGRESQL_USERNAME) ?: throw IllegalStateException(
          "Missing configuration item `" +
            ConfigConstants.INDEX_POSTGRESQL_USERNAME + "'"
        )

      val actualPassword =
        password ?: config.getString(ConfigConstants.INDEX_POSTGRESQL_PASSWORD) ?: throw IllegalStateException(
          "Missing configuration item `" +
            ConfigConstants.INDEX_POSTGRESQL_PASSWORD + "'"
        )

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
      r?.first()?.getJsonObject(0)?.let { ChunkMeta.fromJsonObject(it) }
        ?: throw NoSuchElementException("Could not find chunk metadata with ID `$id' in index")
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

  private suspend fun <T> streamQuery(
    statement: String, params: List<Any>,
    readSize: Int = 50, rowToItem: suspend (Row) -> T
  ): Flow<T> = flow {
    pgClient.withConnection { conn ->
      val preparedStatement = conn.prepare(statement).await()
      val transaction = conn.begin().await()
      try {
        val cursor = preparedStatement.cursor(Tuple.wrap(params))
        try {
          do {
            val rows = cursor.read(readSize).await()
            emitAll(rows.map { rowToItem(it) }.asFlow())
          } while (cursor.hasMore())
        } finally {
          cursor.close()
        }
      } finally {
        transaction.commit()
      }
    }
  }

  override suspend fun getDistinctMeta(query: IndexQuery): Flow<ChunkMeta> {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val statement = "SELECT DISTINCT $DATA->>'$CHUNK_META' FROM $DOCUMENTS WHERE $where"
    return streamQuery(statement, params) { findChunkMeta(it.getString(0)) }
  }

  override suspend fun getMeta(query: IndexQuery): Flow<Pair<String, ChunkMeta>> {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val statement = "SELECT $ID, $DATA->>'$CHUNK_META' FROM $DOCUMENTS WHERE $where"
    return streamQuery(statement, params) { it.getString(0) to findChunkMeta(it.getString(1)) }
  }

  override suspend fun getPaginatedMeta(
    query: IndexQuery, maxPageSize: Int,
    previousScrollId: String?
  ): Index.Page<Pair<String, ChunkMeta>> {
    val (where, whereParams) = PostgreSQLQueryTranslator.translate(query)
    val (statement, params) = if (previousScrollId != null) {
      val params = whereParams + previousScrollId + maxPageSize
      val statement = "SELECT $ID, $DATA->>'$CHUNK_META' FROM $DOCUMENTS " +
          "WHERE $ID > $${whereParams.size + 1} AND ($where) " +
          "ORDER BY $ID LIMIT $${whereParams.size + 2}"
      statement to params
    } else {
      val params = whereParams + maxPageSize
      val statement = "SELECT $ID, $DATA->>'$CHUNK_META' FROM $DOCUMENTS " +
          "WHERE $where ORDER BY $ID LIMIT $${whereParams.size + 1}"
      statement to params
    }
    val items = client.preparedQuery(statement).execute(Tuple.from(params))
      .await().map { it.getString(0) to findChunkMeta(it.getString(1)) }
    val scrollId = items.lastOrNull()?.first
    return Index.Page(items, scrollId)
  }

  override suspend fun getPaths(query: IndexQuery): Flow<String> {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val statement = "SELECT $ID FROM $DOCUMENTS WHERE $where"
    return streamQuery(statement, params) { it.getString(0) }
  }

  override suspend fun addTags(query: IndexQuery, tags: Collection<String>) {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val n = params.size
    val statement = "UPDATE $DOCUMENTS SET " +
      "$DATA = jsonb_set(" +
      "$DATA, '{$TAGS}', to_jsonb(" +
      "ARRAY(" +
      "SELECT DISTINCT jsonb_array_elements(" +
      "COALESCE($DATA->'$TAGS', '[]'::jsonb) || $${n + 1}" +
      ")" +
      ")" +
      ")" +
      ") WHERE $where"
    val paramsList = params.toMutableList()
    paramsList.add(JsonArray(tags.toList()))
    client.preparedQuery(statement).execute(Tuple.wrap(paramsList)).await()
  }

  override suspend fun removeTags(query: IndexQuery, tags: Collection<String>) {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val n = params.size
    val statement = "UPDATE $DOCUMENTS SET $DATA = jsonb_set($DATA, '{$TAGS}', " +
      "COALESCE(($DATA->'$TAGS')::jsonb, '[]'::jsonb) - $${n + 1}::text[]) WHERE $where"
    val paramsList = params.toMutableList()
    paramsList.add(tags.toTypedArray())
    client.preparedQuery(statement).execute(Tuple.wrap(paramsList)).await()
  }

  override suspend fun setProperties(query: IndexQuery, properties: Map<String, Any>) {
    // convert to JSON array
    val props = jsonArrayOf()
    properties.entries.forEach { e ->
      props.add(jsonObjectOf(KEY to e.key, VALUE to e.value))
    }

    // remove properties with these keys (if they exist)
    removeProperties(query, properties.keys)

    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val n = params.size
    val statement = "UPDATE $DOCUMENTS SET $DATA = jsonb_set(" +
      "$DATA, '{$PROPS}', $DATA->'$PROPS' || $${n + 1}" +
      ") WHERE $where"
    val paramsList = params.toMutableList()
    paramsList.add(props)
    client.preparedQuery(statement).execute(Tuple.wrap(paramsList)).await()
  }

  override suspend fun removeProperties(query: IndexQuery, properties: Collection<String>) {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val n = params.size
    val statement = "UPDATE $DOCUMENTS SET " +
      "$DATA = jsonb_set(" +
      "$DATA, '{$PROPS}', to_jsonb(" +
      "ARRAY(" +
      "WITH a AS (SELECT jsonb_array_elements($DATA->'$PROPS') AS c) " +
      "SELECT * FROM a WHERE c->>'$KEY' != ANY($${n + 1})" +
      ")" +
      ")" +
      ") WHERE $where"
    val paramsList = params.toMutableList()
    paramsList.add(properties.toTypedArray())
    client.preparedQuery(statement).execute(Tuple.wrap(paramsList)).await()
  }

  override suspend fun getPropertyValues(query: IndexQuery, propertyName: String): Flow<Any?> {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val n = params.size
    val statement = "WITH p AS (SELECT jsonb_array_elements($DATA->'$PROPS') " +
      "AS a FROM $DOCUMENTS WHERE $where) " +
      "SELECT DISTINCT a->'$VALUE' FROM p WHERE a->'$KEY'=$${n + 1}"
    val paramsList = params.toMutableList()
    paramsList.add(propertyName)
    return streamQuery(statement, paramsList) { it.getValue(0) }
  }

  override suspend fun getAttributeValues(query: IndexQuery, attributeName: String): Flow<Any?> {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val n = params.size
    val statement = "WITH p AS (SELECT jsonb_array_elements($DATA->'$GENATTRS') " +
      "AS a FROM $DOCUMENTS WHERE $where) " +
      "SELECT DISTINCT a->'$VALUE' FROM p WHERE a->'$KEY'=$${n + 1}"
    val paramsList = params.toMutableList()
    paramsList.add(attributeName)
    return streamQuery(statement, paramsList) { it.getValue(0) }
  }

  override suspend fun delete(query: IndexQuery) {
    val (where, params) = PostgreSQLQueryTranslator.translate(query)
    val statement = "DELETE FROM $DOCUMENTS WHERE $where"
    client.preparedQuery(statement).execute(Tuple.from(params)).await()
  }

  override suspend fun delete(paths: Collection<String>) {
    val statement = "DELETE FROM $DOCUMENTS WHERE $ID=ANY($1)"
    val preparedStatement = client.preparedQuery(statement)
    for (chunk in paths.chunked(1000)) {
      val deleteParams = Tuple.of(chunk.toTypedArray())
      preparedStatement.execute(deleteParams).await()
    }
  }

  override suspend fun getLayers(): Flow<String> {
    val statement = "SELECT DISTINCT $DATA->>'$LAYER' FROM $DOCUMENTS"
    return streamQuery(statement, emptyList()) { it.getString(0) }
  }

  override suspend fun existsLayer(name: String): Boolean {
    val (where, params) = PostgreSQLQueryTranslator.translate(StartsWith(LAYER, normalizeLayer(name)))
    val statement = "SELECT TRUE FROM $DOCUMENTS WHERE $where LIMIT 1"
    val r = client.preparedQuery(statement).execute(Tuple.wrap(params)).await()
    return r.size() > 0
  }

  override suspend fun setUpDatabaseIndexes(indexes: List<DatabaseIndex>) {

    fun indexName(idx: DatabaseIndex): String = "$IDX_PREFIX${idx.name.lowercase()}"

    // Check which indexes already exist.
    // We only ever touch indexes, that start with the prefix 'georocket_',
    // so that we do not accidentally remove/overwrite/alter any indexes that the user/dba added
    // manually or the default index on the primary key.
    val existingIndexes =
      client.query("SELECT indexname FROM pg_indexes WHERE tablename = '$DOCUMENTS' AND indexname LIKE '$IDX_PREFIX%'")
        .execute().await().map { it.getString(0) }

    // create new indexes
    val indexesToAdd = indexes.filter { idx ->
      val name = indexName(idx)
      !existingIndexes.contains(name)
    }
    for (idx in indexesToAdd) {
      val using = when (idx) {
        is DatabaseIndex.Array -> "gin((${fieldJsonValue(DATA, idx.field)}) jsonb_path_ops)"
        is DatabaseIndex.ElemMatchExists -> "gin((${fieldJsonValue(DATA, idx.field)}) jsonb_path_ops)"
        is DatabaseIndex.Eq -> "hash((${fieldJsonValue(DATA, idx.field)}))"
        is DatabaseIndex.Geo -> "gist(ST_GeomFromGeoJSON(${fieldJsonValue(DATA, idx.field)}))"
        is DatabaseIndex.Range -> "btree((${fieldJsonValue(DATA, idx.field)}))"
        is DatabaseIndex.StartsWith -> "btree((${fieldStringValue(DATA, idx.field)}) text_pattern_ops)"
        is DatabaseIndex.ElemMatchCompare -> {
          val array = fieldJsonValue(DATA, idx.field)
          val jsonpath = jsonPathToArrayElementField(
            listOf(
              Compare(idx.elemKeyField, idx.keyValue, QueryPart.ComparisonOperator.EQ)
            ), idx.elemValueField
          )
          "btree(jsonb_path_query_first($array, '$jsonpath'))"
        }
      }
      val name = indexName(idx)
      log.info("Creating database index $name")
      val statement = "CREATE INDEX $name ON $DOCUMENTS USING $using"
      log.debug("Index definition: $statement")
      client.query(statement).execute().await()
    }

    // Remove indexes that are not present anymore.
    val indexesToRemove =
      existingIndexes.filter { index_name ->
        indexes.none { indexName(it) == index_name }
      }
    for (name in indexesToRemove) {
      log.info("Dropping database index $name")
      client.query("DROP INDEX $name").execute().await()
    }

    // Refresh statistics
    if (indexesToAdd.isNotEmpty() || indexesToRemove.isNotEmpty()) {
      log.debug("Executing ANALYZE")
      client.query("ANALYZE").execute().await()
    }
  }
}
