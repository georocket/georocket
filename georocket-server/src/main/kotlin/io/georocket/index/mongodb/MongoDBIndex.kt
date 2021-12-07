package io.georocket.index.mongodb

import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoCollection
import com.mongodb.reactivestreams.client.MongoDatabase
import io.georocket.constants.ConfigConstants.EMBEDDED_MONGODB_STORAGE_PATH
import io.georocket.constants.ConfigConstants.INDEX_MONGODB_CONNECTION_STRING
import io.georocket.constants.ConfigConstants.INDEX_MONGODB_EMBEDDED
import io.georocket.index.Index
import io.georocket.storage.ChunkMeta
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.MimeTypeUtils
import io.georocket.util.aggregateAwait
import io.georocket.util.countDocumentsAwait
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

class MongoDBIndex private constructor() : Index {
  companion object {
    private const val INTERNAL_ID = "_id"
    private const val CHUNK_META = "chunkMeta"
    private const val COLL_DOCUMENTS = "documents"
    private const val COLL_COLLECTIONS = "ogcapifeatures.collections"

    suspend fun create(vertx: Vertx, connectionString: String? = null,
        storagePath: String? = null): MongoDBIndex {
      val r = MongoDBIndex()
      r.start(vertx, connectionString, storagePath)
      return r
    }
  }

  private lateinit var client: MongoClient
  private lateinit var db: MongoDatabase

  private lateinit var collDocuments: MongoCollection<JsonObject>
  private lateinit var collCollections: MongoCollection<JsonObject>

  private suspend fun start(vertx: Vertx, connectionString: String?,
      storagePath: String?) {
    val config = vertx.orCreateContext.config()

    val embedded = config.getBoolean(INDEX_MONGODB_EMBEDDED, false)
    if (embedded) {
      val actualStoragePath = storagePath ?: config.getString(
        EMBEDDED_MONGODB_STORAGE_PATH) ?:
          throw IllegalStateException("Missing configuration item `" +
              EMBEDDED_MONGODB_STORAGE_PATH + "'")
      client = SharedMongoClient.createEmbedded(vertx, actualStoragePath)
      db = client.getDatabase(SharedMongoClient.DEFAULT_EMBEDDED_DATABASE)
    } else {
      val actualConnectionString = connectionString ?:
        config.getString(INDEX_MONGODB_CONNECTION_STRING) ?:
        throw IllegalArgumentException("Missing configuration item `" +
            INDEX_MONGODB_CONNECTION_STRING + "'")
      val cs = ConnectionString(actualConnectionString)
      client = SharedMongoClient.create(cs)
      db = client.getDatabase(cs.database)
    }

    collDocuments = db.getCollection(COLL_DOCUMENTS, JsonObject::class.java)
    collCollections = db.getCollection(COLL_COLLECTIONS, JsonObject::class.java)
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

  /**
   * Extract a path string and a [ChunkMeta] object from a given [hit] object
   */
  private fun createChunkMeta(hit: JsonObject): Pair<String, ChunkMeta> {
    val path = hit.getString(INTERNAL_ID)
    val cm = hit.getJsonObject(CHUNK_META)
    val mimeType = cm.getString("mimeType", XMLChunkMeta.MIME_TYPE)
    return path to if (MimeTypeUtils.belongsTo(mimeType, "application", "xml") ||
      MimeTypeUtils.belongsTo(mimeType, "text", "xml")) {
      XMLChunkMeta(cm)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "geo+json")) {
      GeoJsonChunkMeta(cm)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "json")) {
      JsonChunkMeta(cm)
    } else {
      ChunkMeta(cm)
    }
  }

  override suspend fun getMeta(query: JsonObject): List<Pair<String, ChunkMeta>> {
    val results = collDocuments.findAwait(query, projection = json {
      obj(
        CHUNK_META to 1
      )
    })
    return results.map { createChunkMeta(it) }
  }

  override suspend fun getPaths(query: JsonObject): List<String> {
    val results = collDocuments.findAwait(query, projection = json {
      obj(
        INTERNAL_ID to 1
      )
    })
    return results.map { it.getString(INTERNAL_ID) }
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

  override suspend fun delete(query: JsonObject) {
    collDocuments.deleteManyAwait(query)
  }

  /**
   * Get a list of all collections
   */
  override suspend fun getCollections(): List<String> {
    return collCollections.findAwait(jsonObjectOf()).map { it.getString("name") }
  }

  /**
   * Add a collection with a given [name]
   */
  override suspend fun addCollection(name: String) {
    collCollections.insertOneAwait(jsonObjectOf("name" to name))
  }

  /**
   * Test if a collection with a given [name] exists
   */
  override suspend fun existsCollection(name: String): Boolean {
    return collCollections.countDocumentsAwait(jsonObjectOf("name" to name)) > 0
  }

  /**
   * Delete one or more collections with the given [name]
   */
  override suspend fun deleteCollection(name: String) {
    collCollections.deleteManyAwait(jsonObjectOf("name" to name))
  }
}
