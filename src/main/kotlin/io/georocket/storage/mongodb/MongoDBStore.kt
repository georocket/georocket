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
import io.georocket.constants.ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING
import io.georocket.index.mongodb.SharedMongoClient
import io.georocket.storage.Store
import io.georocket.util.coFind
import io.georocket.util.deleteManyAwait
import io.georocket.util.insertManyAwait
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.core.json.jsonObjectOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
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

    val actualConnectionString = connectionString ?:
      config.getString(STORAGE_MONGODB_CONNECTION_STRING) ?:
      config.getString(INDEX_MONGODB_CONNECTION_STRING)
    if (actualConnectionString == null) {
      val actualStoragePath = storagePath ?: config.getString(
        EMBEDDED_MONGODB_STORAGE_PATH) ?:
          throw IllegalStateException("Missing configuration item `" +
              EMBEDDED_MONGODB_STORAGE_PATH + "'")
      client = SharedMongoClient.createEmbedded(vertx, actualStoragePath)
      db = client.getDatabase(SharedMongoClient.DEFAULT_EMBEDDED_DATABASE)
    } else {
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
    val chunks = collChunks.coFind(jsonObjectOf("filename" to path),
      sort = jsonObjectOf("n" to 1))
    var found = false
    val result = Buffer.buffer()
    chunks.collect { c ->
      found = true
      val data = c.getBinary("data")
      result.appendBuffer(Buffer.buffer(data.data))
    }
    if (!found) {
      throw NoSuchElementException("Could not find chunk with path `$path'")
    }
    return result
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
    if (chunks.isNotEmpty()) {
      val chunksToAdd = chunks.flatMap { chunkToBson(it.first, it.second) }
      collChunks.insertManyAwait(chunksToAdd)
    }
  }

  override suspend fun delete(paths: Flow<String>): Long {
    var result = 0L
    var len = 0
    val chunk = mutableListOf<String>()

    val doDelete = suspend {
      val dr = collChunks.deleteManyAwait(jsonObjectOf(
        "filename" to jsonObjectOf(
          "\$in" to chunk
        )
      ))
      result += dr.deletedCount
    }

    paths.collect { p ->
      chunk.add(p)

      // keep size of 'chunk' at around 5 MB (16 MB is MongoDB's default
      // maximum document size)
      len += p.length
      if (len >= 1024 * 1024 * 5) {
        doDelete()
        chunk.clear()
        len = 0
      }
    }

    if (chunk.isNotEmpty()) {
      doDelete()
    }

    return result
  }
}
