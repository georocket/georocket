package io.georocket.index.mongodb

import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoCollection
import com.mongodb.reactivestreams.client.MongoDatabase
import io.georocket.constants.ConfigConstants
import io.georocket.index.Index
import io.georocket.util.aggregateAwait
import io.georocket.util.deleteManyAwait
import io.georocket.util.findAwait
import io.georocket.util.insertManyAwait
import io.georocket.util.insertOneAwait
import io.georocket.util.updateManyAwait
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.core.json.obj

class MongoDBIndex(vertx: Vertx, connectionString: String? = null) : Index {
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
    val config = vertx.orCreateContext.config()

    val actualConnectionString = connectionString ?:
      config.getString(ConfigConstants.INDEX_MONGODB_CONNECTION_STRING) ?:
      config.getString(ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING) ?:
      throw IllegalArgumentException("Missing configuration item `" +
          ConfigConstants.INDEX_MONGODB_CONNECTION_STRING + "' or `" +
          ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING + "'")

    val cs = ConnectionString(actualConnectionString)
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

  override suspend fun addMany(docs: Collection<Pair<String, JsonObject>>) {
    val copies = docs.map { d ->
      val copy = d.second.copy()
      copy.put(INTERNAL_ID, d.first)
      copy
    }
    collDocuments.insertManyAwait(copies)
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

  override suspend fun addTags(query: JsonObject, tags: Collection<String>) {
    collDocuments.updateManyAwait(query, jsonObjectOf(
      "\$addToSet" to jsonObjectOf(
        "tags" to jsonObjectOf(
          "\$each" to tags
        )
      )
    ))
  }

  override suspend fun removeTags(query: JsonObject, tags: Collection<String>) {
    collDocuments.updateManyAwait(query, jsonObjectOf(
      "\$pull" to jsonObjectOf(
        "tags" to jsonObjectOf(
          "\$in" to tags
        )
      )
    ))
  }

  override suspend fun setProperties(query: JsonObject, properties: Map<String, Any>) {
    // convert to key-value pairs
    val props = properties.entries.map { e ->
      mapOf("key" to e.key, "value" to e.value)
    }

    // remove properties with these keys (if they exist)
    removeProperties(query, properties.keys)

    // now insert them (again or for the first time)
    collDocuments.updateManyAwait(query, jsonObjectOf(
      "\$push" to jsonObjectOf(
        "props" to jsonObjectOf(
          "\$each" to props
        )
      )
    ))
  }

  override suspend fun removeProperties(query: JsonObject, properties: Collection<String>) {
    collDocuments.updateManyAwait(query, jsonObjectOf(
      "\$pull" to jsonObjectOf(
        "props" to jsonObjectOf(
          "key" to jsonObjectOf(
            "\$in" to properties.toList()
          )
        )
      )
    ))
  }

  override suspend fun getPropertyValues(query: JsonObject, propertyName: String): List<Any?> {
    val result = collDocuments.aggregateAwait(listOf(
      jsonObjectOf("\$unwind" to jsonObjectOf(
        "path" to "\$props"
      )),
      jsonObjectOf("\$match" to jsonObjectOf(
        "props.key" to propertyName
      )),
      jsonObjectOf("\$group" to jsonObjectOf(
        "_id" to null,
        "values" to jsonObjectOf(
          "\$addToSet" to "\$props.value"
        )
      ))
    ))
    return result.firstOrNull()?.getJsonArray("values")?.list ?: emptyList()
  }

  override suspend fun getAttributeValues(query: JsonObject, attributeName: String): List<Any?> {
    val result = collDocuments.aggregateAwait(listOf(
      jsonObjectOf("\$unwind" to jsonObjectOf(
        "path" to "\$genAttrs"
      )),
      jsonObjectOf("\$match" to jsonObjectOf(
        "genAttrs.key" to attributeName
      )),
      jsonObjectOf("\$group" to jsonObjectOf(
        "_id" to null,
        "values" to jsonObjectOf(
          "\$addToSet" to "\$genAttrs.value"
        )
      ))
    ))
    return result[0].getJsonArray("values").list
  }

  override suspend fun delete(ids: Collection<String>) {
    collDocuments.deleteManyAwait(json {
      obj(
        INTERNAL_ID to obj(
          "\$in" to ids
        )
      )
    })
  }
}
