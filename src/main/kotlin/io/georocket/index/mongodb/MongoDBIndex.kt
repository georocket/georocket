package io.georocket.index.mongodb

import com.github.benmanes.caffeine.cache.Caffeine
import com.mongodb.ConnectionString
import com.mongodb.client.model.IndexModel
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.ReturnDocument
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoCollection
import com.mongodb.reactivestreams.client.MongoDatabase
import io.georocket.constants.ConfigConstants.EMBEDDED_MONGODB_STORAGE_PATH
import io.georocket.constants.ConfigConstants.INDEX_MONGODB_CONNECTION_STRING
import io.georocket.index.Index
import io.georocket.storage.ChunkMeta
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.MimeTypeUtils
import io.georocket.util.UniqueID
import io.georocket.util.aggregateAwait
import io.georocket.util.coDistinct
import io.georocket.util.coFind
import io.georocket.util.countDocumentsAwait
import io.georocket.util.deleteManyAwait
import io.georocket.util.findOneAndUpdateAwait
import io.georocket.util.findOneAwait
import io.georocket.util.getAwait
import io.georocket.util.insertManyAwait
import io.georocket.util.insertOneAwait
import io.georocket.util.updateManyAwait
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.core.json.obj
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.awaitFirstOrNull
import java.time.Duration

class MongoDBIndex private constructor() : Index {
  companion object {
    private const val INTERNAL_ID = "_id"
    private const val CHUNK_META = "chunkMeta"
    private const val COLL_DOCUMENTS = "documents"
    private const val COLL_CHUNKMETA = "chunkMeta"
    private const val COLL_COLLECTIONS = "ogcapifeatures.collections"

    suspend fun create(vertx: Vertx, connectionString: String? = null,
        storagePath: String? = null): MongoDBIndex {
      val r = MongoDBIndex()
      r.start(vertx, connectionString, storagePath)
      return r
    }

    /**
     * A cache used when importing chunk metadata, so that the same objects are
     * not inserted over and over again
     */
    private val addedChunkMetaCache = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(Duration.ofDays(1))
      .buildAsync<ChunkMeta, String>()

    /**
     * A cache used when loading chunk metadata by ID
     */
    private val loadedChunkMetaCache = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(Duration.ofDays(1))
      .buildAsync<String, ChunkMeta>()
  }

  private lateinit var client: MongoClient
  private lateinit var db: MongoDatabase

  private lateinit var collDocuments: MongoCollection<JsonObject>
  private lateinit var collChunkMeta: MongoCollection<JsonObject>
  private lateinit var collCollections: MongoCollection<JsonObject>

  private suspend fun start(vertx: Vertx, connectionString: String?,
      storagePath: String?) {
    val config = vertx.orCreateContext.config()

    val actualConnectionString = connectionString ?:
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

    collDocuments = db.getCollection(COLL_DOCUMENTS, JsonObject::class.java)
    collChunkMeta = db.getCollection(COLL_CHUNKMETA, JsonObject::class.java)
    collCollections = db.getCollection(COLL_COLLECTIONS, JsonObject::class.java)

    collDocuments.createIndexes(listOf(IndexModel(Indexes.ascending(CHUNK_META),
      IndexOptions().background(true)),
    )).awaitFirstOrNull()
    collChunkMeta.createIndexes(listOf(IndexModel(Indexes.ascending(CHUNK_META),
      IndexOptions().unique(true).background(true)),
    )).awaitFirstOrNull()
  }

  override suspend fun close() {
    client.close()
  }

  private suspend fun addOrGetChunkMeta(meta: ChunkMeta): String {
    return addedChunkMetaCache.getAwait(meta) {
      val metaJson = meta.toJsonObject()
      val result = collChunkMeta.findOneAndUpdateAwait(jsonObjectOf(
        CHUNK_META to metaJson
      ), jsonObjectOf(
        "\$setOnInsert" to jsonObjectOf(
          INTERNAL_ID to UniqueID.next()
        )
      ), true, ReturnDocument.AFTER, jsonObjectOf(INTERNAL_ID to 1))
      result!!.getString(INTERNAL_ID)
    }
  }

  private suspend fun findChunkMeta(id: String): ChunkMeta {
    return loadedChunkMetaCache.getAwait(id) {
      val o = collChunkMeta.findOneAwait(jsonObjectOf(INTERNAL_ID to id))
        ?: throw NoSuchElementException("Could not find chunk metadata with ID `$id' in index")
      createChunkMeta(o.getJsonObject(CHUNK_META))
    }
  }

  override suspend fun addMany(docs: Collection<Index.AddManyParam>) {
    val copies = docs.map { d ->
      val chunkMetaId = addOrGetChunkMeta(d.meta)
      val copy = d.doc.copy()
      copy.put(INTERNAL_ID, d.path)
      copy.put(CHUNK_META, chunkMetaId)
      copy
    }
    collDocuments.insertManyAwait(copies)
  }

  /**
   * Extract a path string and a [ChunkMeta] object from a given [cm] object
   */
  private fun createChunkMeta(cm: JsonObject): ChunkMeta {
    val mimeType = cm.getString("mimeType", XMLChunkMeta.MIME_TYPE)
    return if (MimeTypeUtils.belongsTo(mimeType, "application", "xml") ||
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

  override suspend fun getDistinctMeta(query: JsonObject): Flow<ChunkMeta> {
    return collDocuments.coDistinct(CHUNK_META, query, String::class.java)
      .map { findChunkMeta(it) }
  }

  override suspend fun getMeta(query: JsonObject): Flow<Pair<String, ChunkMeta>> {
    val results = collDocuments.coFind(query, projection = json {
      obj(
        CHUNK_META to 1
      )
    })
    return results.map { hit ->
      val path = hit.getString(INTERNAL_ID)
      val cmId = hit.getString(CHUNK_META)
      path to findChunkMeta(cmId)
    }
  }

  override suspend fun getPaths(query: JsonObject): Flow<String> {
    val results = collDocuments.coFind(query, projection = json {
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
  override suspend fun getCollections(): Flow<String> {
    return collCollections.coFind(jsonObjectOf()).map { it.getString("name") }
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
