package io.georocket.storage.mongodb

import com.mongodb.ConnectionString
import com.mongodb.client.model.IndexModel
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoCollection
import com.mongodb.reactivestreams.client.MongoDatabase
import io.georocket.constants.ConfigConstants.EMBEDDED_MONGODB_STORAGE_PATH
import io.georocket.constants.ConfigConstants.INDEX_MONGODB_CONNECTION_STRING
import io.georocket.constants.ConfigConstants.INDEX_MONGODB_EMBEDDED
import io.georocket.constants.ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING
import io.georocket.constants.ConfigConstants.STORAGE_MONGODB_EMBEDDED
import io.georocket.index.mongodb.SharedMongoClient
import io.georocket.storage.IndexMeta
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.georocket.util.deleteManyAwait
import io.georocket.util.findAwait
import io.georocket.util.insertManyAwait
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.core.json.jsonObjectOf
import kotlinx.coroutines.reactive.awaitSingleOrNull
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import java.lang.Integer.min

/**
 * Stores chunks in MongoDB
 * @author Michel Kraemer
 */
class MongoDBStore private constructor() : Store {
  companion object {
    private const val MAX_BINARY_DATA_SIZE = 1024 * 1024 * 15

    suspend fun create(vertx: Vertx, connectionString: String? = null,
        storagePath: String? = null): MongoDBStore {
      val r = MongoDBStore()
      r.start(vertx, connectionString, storagePath)
      return r
    }
  }

  private lateinit var client: MongoClient
  private lateinit var db: MongoDatabase
  private lateinit var collChunks: MongoCollection<BsonDocument>

  private suspend fun start(vertx: Vertx, connectionString: String?,
      storagePath: String?) {
    val config = vertx.orCreateContext.config()

    val embedded = config.getBoolean(STORAGE_MONGODB_EMBEDDED) ?:
      config.getBoolean(INDEX_MONGODB_EMBEDDED) ?: false
    if (embedded) {
      val actualStoragePath = storagePath ?: config.getString(
        EMBEDDED_MONGODB_STORAGE_PATH) ?:
          throw IllegalStateException("Missing configuration item `" +
              EMBEDDED_MONGODB_STORAGE_PATH + "'")
      client = SharedMongoClient.createEmbedded(vertx, actualStoragePath)
      db = client.getDatabase(SharedMongoClient.DEFAULT_EMBEDDED_DATABASE)
    } else {
      val actualConnectionString = connectionString ?:
        config.getString(STORAGE_MONGODB_CONNECTION_STRING) ?:
        config.getString(INDEX_MONGODB_CONNECTION_STRING) ?:
        throw IllegalArgumentException("Missing configuration item `" +
            STORAGE_MONGODB_CONNECTION_STRING + "' or `" +
            INDEX_MONGODB_CONNECTION_STRING + "'")
      val cs = ConnectionString(actualConnectionString)
      client = SharedMongoClient.create(cs)
      db = client.getDatabase(cs.database)
    }

    collChunks = db.getCollection("chunks", BsonDocument::class.java)

    collChunks.createIndexes(listOf(
      IndexModel(Indexes.compoundIndex(
        Indexes.ascending("filename"),
        Indexes.ascending("n")
      ), IndexOptions().background(true).unique(true)),
    )).awaitSingleOrNull()
  }

  override fun close() {
    client.close()
  }

  override suspend fun getOne(path: String): Buffer {
    val chunks = collChunks.findAwait(jsonObjectOf("filename" to path),
      sort = jsonObjectOf("n" to 1))
    if (chunks.isEmpty()) {
      throw NoSuchElementException("Could not find chunk with path `$path'")
    }

    val result = Buffer.buffer()
    chunks.forEach { c ->
      val data = c.getBinary("data")
      result.appendBuffer(Buffer.buffer(data.data))
    }
    return result
  }

  override fun makePath(indexMetadata: IndexMeta, layer: String): String {
    val path = layer.ifEmpty { "/" }
    return PathUtils.join(path, indexMetadata.correlationId + UniqueID.next())
  }

  private fun chunkToBson(chunk: Buffer, path: String): List<BsonDocument> {
    val result = mutableListOf<BsonDocument>()
    val bytes = chunk.bytes
    var pos = 0
    var n = 0
    while (pos < bytes.size) {
      val len = min(bytes.size - pos, MAX_BINARY_DATA_SIZE)
      val range = if (pos == 0 && len == bytes.size) {
        bytes
      } else {
        bytes.copyOfRange(pos, pos + len)
      }
      val doc = BsonDocument()
      doc["filename"] = BsonString(path)
      doc["n"] = BsonInt32(n)
      doc["data"] = BsonBinary(range)
      pos += MAX_BINARY_DATA_SIZE
      n++
      result.add(doc)
    }
    return result
  }

  override suspend fun add(chunk: Buffer, path: String) {
    collChunks.insertManyAwait(chunkToBson(chunk, path))
  }

  override suspend fun addMany(chunks: Collection<Pair<Buffer, String>>) {
    val chunksToAdd = chunks.flatMap { chunkToBson(it.first, it.second) }
    collChunks.insertManyAwait(chunksToAdd)
  }

  override suspend fun delete(paths: Collection<String>) {
    collChunks.deleteManyAwait(jsonObjectOf(
      "filename" to jsonObjectOf(
        "\$in" to paths
      )
    ))
  }
}
