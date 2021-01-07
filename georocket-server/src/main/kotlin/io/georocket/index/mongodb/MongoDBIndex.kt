package io.georocket.index.mongodb

import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoCollection
import com.mongodb.reactivestreams.client.MongoDatabase
import io.georocket.index.Index
import io.georocket.util.insertOneAwait
import io.vertx.core.json.JsonObject

class MongoDBIndex : Index {
  companion object {
    private const val INTERNAL_ID = "_id"
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
    collDocuments.insertOneAwait(doc)
  }
}
