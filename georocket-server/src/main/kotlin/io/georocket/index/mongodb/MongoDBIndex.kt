package io.georocket.index.mongodb

import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoCollection
import com.mongodb.reactivestreams.client.MongoDatabase
import io.georocket.index.Index
import io.georocket.util.deleteManyAwait
import io.georocket.util.findAwait
import io.georocket.util.insertOneAwait
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj

class MongoDBIndex : Index {
  companion object {
    private const val ID = "id"
    private const val INTERNAL_ID = "_id"
    private const val CHUNK_META = "chunkMeta"
    private const val COLL_DOCUMENTS = "documents"
  }

  private val client: MongoClient
  private val db: MongoDatabase

  private val collDocuments: MongoCollection<JsonObject>

  init {
    // TODO make connectionString configurable
    val connectionString = "mongodb://localhost:27017/georocket"

    val cs = ConnectionString(connectionString)
    client = SharedMongoClient.create(cs)
    db = client.getDatabase(cs.database)

    collDocuments = db.getCollection(COLL_DOCUMENTS, JsonObject::class.java)
  }

  override suspend fun close() {
    client.close()
  }

  override suspend fun add(id: String, doc: JsonObject) {
    val copy = doc.copy()
    copy.put(INTERNAL_ID, id)
    collDocuments.insertOneAwait(copy)
  }

  override suspend fun getMeta(query: JsonObject): List<JsonObject> {
    val results = collDocuments.findAwait(query, projection = json {
      obj(
        CHUNK_META to 1
      )
    })
    return results.map {
      val id = it.getString(INTERNAL_ID)
      val cm = it.getJsonObject(CHUNK_META)
      cm.put(ID, id)
      cm
    }
  }

  override suspend fun delete(ids: List<String>) {
    collDocuments.deleteManyAwait(json {
      obj(
        INTERNAL_ID to obj(
          "\$in" to ids
        )
      )
    })
  }
}
