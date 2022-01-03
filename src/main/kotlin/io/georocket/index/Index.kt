package io.georocket.index

import io.georocket.storage.ChunkMeta
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.flow.Flow

interface Index {
  data class AddManyParam(val path: String, val doc: JsonObject, val meta: ChunkMeta)

  suspend fun close()

  suspend fun addMany(docs: Collection<AddManyParam>)

  suspend fun getDistinctMeta(query: JsonObject): Flow<ChunkMeta>

  suspend fun getMeta(query: JsonObject): Flow<Pair<String, ChunkMeta>>

  suspend fun getPaths(query: JsonObject): Flow<String>

  suspend fun addTags(query: JsonObject, tags: Collection<String>)

  suspend fun removeTags(query: JsonObject, tags: Collection<String>)

  suspend fun setProperties(query: JsonObject, properties: Map<String, Any>)

  suspend fun removeProperties(query: JsonObject, properties: Collection<String>)

  suspend fun getPropertyValues(query: JsonObject, propertyName: String): List<Any?>

  suspend fun getAttributeValues(query: JsonObject, attributeName: String): List<Any?>

  suspend fun delete(query: JsonObject)

  suspend fun delete(paths: Collection<String>)

  suspend fun getCollections(): Flow<String>

  suspend fun addCollection(name: String)

  suspend fun existsCollection(name: String): Boolean

  suspend fun deleteCollection(name: String)
}
