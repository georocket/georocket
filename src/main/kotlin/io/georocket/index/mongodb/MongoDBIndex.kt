package io.georocket.index.mongodb

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
import io.georocket.index.AbstractIndex
import io.georocket.index.Index
import io.georocket.index.mongodb.MongoDBQueryTranslator.translate
import io.georocket.query.IndexQuery
import io.georocket.storage.ChunkMeta
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
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.core.json.obj
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.awaitFirstOrNull

class MongoDBIndex private constructor() : Index, AbstractIndex() {
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

  override suspend fun getDistinctMeta(query: IndexQuery): Flow<ChunkMeta> {
    return collDocuments.coDistinct(CHUNK_META, translate(query), String::class.java)
      .map { findChunkMeta(it) }
  }

  override suspend fun getMeta(query: IndexQuery): Flow<Pair<String, ChunkMeta>> {
    val results = collDocuments.coFind(translate(query), projection = json {
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

  override suspend fun getPaginatedMeta(query: IndexQuery, maxPageSize: Int,
      previousScrollId: String?): Index.Page<Pair<String, ChunkMeta>> {
    val filter = if (previousScrollId != null) {
      jsonObjectOf("\$and" to jsonArrayOf(
        jsonObjectOf("_id" to jsonObjectOf("\$gt" to previousScrollId)),
        translate(query)
      ))
    } else {
      translate(query)
    }
    val items = collDocuments.coFind(filter,
      limit = maxPageSize,
      sort = jsonObjectOf(INTERNAL_ID to 1),
      projection = jsonObjectOf(CHUNK_META to 1, INTERNAL_ID to 1)
    ).map { hit ->
      val path = hit.getString(INTERNAL_ID)
      val cmId = hit.getString(CHUNK_META)
      path to findChunkMeta(cmId)
    }.toList()
    val lastId = items.lastOrNull()?.first

    return Index.Page(items, lastId)
  }


  override suspend fun getPaths(query: IndexQuery): Flow<String> {
    val results = collDocuments.coFind(translate(query), projection = json {
      obj(
        INTERNAL_ID to 1
      )
    })
    return results.map { it.getString(INTERNAL_ID) }
  }

  override suspend fun addTags(query: IndexQuery, tags: Collection<String>) {
    collDocuments.updateManyAwait(translate(query), jsonObjectOf(
      "\$addToSet" to jsonObjectOf(
        "tags" to jsonObjectOf(
          "\$each" to tags
        )
      )
    ))
  }

  override suspend fun removeTags(query: IndexQuery, tags: Collection<String>) {
    collDocuments.updateManyAwait(translate(query), jsonObjectOf(
      "\$pull" to jsonObjectOf(
        "tags" to jsonObjectOf(
          "\$in" to tags
        )
      )
    ))
  }

  override suspend fun setProperties(query: IndexQuery, properties: Map<String, Any>) {
    // convert to key-value pairs
    val props = properties.entries.map { e ->
      mapOf("key" to e.key, "value" to e.value)
    }

    // remove properties with these keys (if they exist)
    removeProperties(query, properties.keys)

    // now insert them (again or for the first time)
    collDocuments.updateManyAwait(translate(query), jsonObjectOf(
      "\$push" to jsonObjectOf(
        "props" to jsonObjectOf(
          "\$each" to props
        )
      )
    ))
  }

  override suspend fun removeProperties(query: IndexQuery, properties: Collection<String>) {
    collDocuments.updateManyAwait(translate(query), jsonObjectOf(
      "\$pull" to jsonObjectOf(
        "props" to jsonObjectOf(
          "key" to jsonObjectOf(
            "\$in" to properties.toList()
          )
        )
      )
    ))
  }

  override suspend fun getPropertyValues(query: IndexQuery, propertyName: String): Flow<Any?> {
    val result = collDocuments.aggregateAwait(listOf(
      jsonObjectOf("\$match" to translate(query)),
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
      )),
      jsonObjectOf("\$unwind" to "\$values")
    ))
    return result.map { it.getValue("values") }
  }

  override suspend fun getAttributeValues(query: IndexQuery, attributeName: String): Flow<Any?> {
    val result = collDocuments.aggregateAwait(listOf(
      jsonObjectOf("\$match" to translate(query)),
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
      )),
      jsonObjectOf("\$unwind" to "\$values")
    ))
    return result.map { it.getValue("values") }
  }

  override suspend fun delete(query: IndexQuery) {
    collDocuments.deleteManyAwait(translate(query))
  }

  override suspend fun delete(paths: Collection<String>) {
    var len = 0
    val chunk = mutableListOf<String>()

    val doDelete = suspend {
      collDocuments.deleteManyAwait(jsonObjectOf(
        INTERNAL_ID to jsonObjectOf(
          "\$in" to chunk
        )
      ))
    }

    for (p in paths) {
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
