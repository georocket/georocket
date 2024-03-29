package io.georocket.storage.postgresql

import io.georocket.constants.ConfigConstants
import io.georocket.index.postgresql.PostgreSQLClient
import io.georocket.storage.Store
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.coroutines.await
import io.vertx.sqlclient.Tuple

class PostgreSQLStore private constructor(vertx: Vertx, url: String,
    username: String, password: String) : Store, PostgreSQLClient(vertx, url, username, password) {
  companion object {
    /**
     * Table and column names
     */
    private const val CHUNKS = "chunks"

    suspend fun create(vertx: Vertx, url: String? = null, username: String? = null,
        password: String? = null): PostgreSQLStore {
      val config = vertx.orCreateContext.config()

      val actualUrl = url ?:
        config.getString(ConfigConstants.STORAGE_POSTGRESQL_URL) ?:
        config.getString(ConfigConstants.INDEX_POSTGRESQL_URL) ?:
        throw IllegalStateException("Missing configuration items `" +
            ConfigConstants.STORAGE_POSTGRESQL_URL + "' or `" +
            ConfigConstants.INDEX_POSTGRESQL_URL + "'")

      val actualUsername = username ?:
        config.getString(ConfigConstants.STORAGE_POSTGRESQL_USERNAME) ?:
        config.getString(ConfigConstants.INDEX_POSTGRESQL_USERNAME) ?:
        throw IllegalStateException("Missing configuration items `" +
            ConfigConstants.STORAGE_POSTGRESQL_USERNAME + "' or `" +
            ConfigConstants.INDEX_POSTGRESQL_USERNAME + "'")

      val actualPassword = password ?:
        config.getString(ConfigConstants.STORAGE_POSTGRESQL_PASSWORD) ?:
        config.getString(ConfigConstants.INDEX_POSTGRESQL_PASSWORD) ?:
        throw IllegalStateException("Missing configuration items `" +
            ConfigConstants.STORAGE_POSTGRESQL_PASSWORD + "' or " +
            ConfigConstants.INDEX_POSTGRESQL_PASSWORD + "'")

      return vertx.executeBlocking<PostgreSQLStore> { p ->
        p.complete(PostgreSQLStore(vertx, actualUrl, actualUsername, actualPassword))
      }.await()
    }
  }

  override suspend fun close() {
    super<PostgreSQLClient>.close()
  }

  override suspend fun add(chunk: Buffer, path: String) {
    val statement = "INSERT INTO $CHUNKS ($ID, $DATA) VALUES ($1, $2)"
    val params = Tuple.of(path, chunk)
    client.preparedQuery(statement).execute(params).await()
  }

  override suspend fun getOne(path: String): Buffer {
    val statement = "SELECT $DATA FROM $CHUNKS WHERE $ID=$1"
    val params = Tuple.of(path)
    val rs = client.preparedQuery(statement).execute(params).await()
    return rs?.firstOrNull()?.getBuffer(0) ?:
      throw NoSuchElementException("Could not find chunk with path `$path'")
  }

  override suspend fun getMany(paths: List<String>): Map<String, Buffer> {
    val statement = "SELECT $ID, $DATA FROM $CHUNKS WHERE $ID=$1"
    val params = paths.map { path -> Tuple.of(path) }
    var batchQueryResults = client.preparedQuery(statement).executeBatch(params).await()
    val results = mutableMapOf<String, Buffer>()
    while (batchQueryResults != null) {
      batchQueryResults.forEach { row ->
        val path = row.getString(0)
        val data = row.getBuffer(1)
        results[path] = data
      }
      batchQueryResults = batchQueryResults.next()
    }
    if (results.size < paths.size) {
      val missing = paths.find { !results.containsKey(it) }
      throw NoSuchElementException("Could not find chunk with path `$missing'")
    }
    return results
  }

  override suspend fun delete(paths: Collection<String>): Long {
    var result = 0L
    val chunk = mutableListOf<String>()

    val doDelete = suspend {
      val deleteParams = Tuple.of(chunk.toTypedArray())
      val statement3 = "WITH deleted AS (DELETE FROM $CHUNKS WHERE $ID=ANY($1) RETURNING $ID) " +
          "SELECT COUNT(*) FROM deleted"
      val dr = client.preparedQuery(statement3).execute(deleteParams).await()
      result += dr.first().getLong(0)
    }

    for (p in paths) {
      chunk.add(p)
      if (chunk.size == 1000) {
        doDelete()
        chunk.clear()
      }
    }

    if (chunk.isNotEmpty()) {
      doDelete()
    }

    return result
  }
}
