package io.georocket.index

import io.georocket.storage.ChunkMeta
import io.vertx.core.json.JsonObject

interface Index {
  suspend fun close()

  suspend fun add(id: String, doc: JsonObject)

  suspend fun addMany(docs: Collection<Pair<String, JsonObject>>)

  suspend fun getDistinctMeta(query: JsonObject): List<ChunkMeta>

  suspend fun getMeta(query: JsonObject): List<Pair<String, ChunkMeta>>

  suspend fun getPaths(query: JsonObject): List<String>

  suspend fun addTags(query: JsonObject, tags: Collection<String>)

  suspend fun removeTags(query: JsonObject, tags: Collection<String>)

  suspend fun setProperties(query: JsonObject, properties: Map<String, Any>)

  suspend fun removeProperties(query: JsonObject, properties: Collection<String>)

  suspend fun getPropertyValues(query: JsonObject, propertyName: String): List<Any?>

  suspend fun getAttributeValues(query: JsonObject, attributeName: String): List<Any?>

  suspend fun delete(query: JsonObject)

  suspend fun getCollections(): List<String>

  suspend fun addCollection(name: String)

  suspend fun existsCollection(name: String): Boolean

  suspend fun deleteCollection(name: String)
}
