package io.georocket.index

import io.georocket.query.IndexQuery
import io.georocket.storage.ChunkMeta
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.flow.Flow

interface Index {
  data class AddManyParam(val path: String, val doc: JsonObject, val meta: ChunkMeta)
  data class Page<T>(val items: List<T>, val scrollId: String?)

  suspend fun close()

  suspend fun addMany(docs: Collection<AddManyParam>)

  suspend fun getDistinctMeta(query: IndexQuery): Flow<ChunkMeta>

  suspend fun getMeta(query: IndexQuery): Flow<Pair<String, ChunkMeta>>

  suspend fun getPaginatedMeta(
    query: IndexQuery, maxPageSize: Int,
    previousScrollId: String?
  ): Page<Pair<String, ChunkMeta>>

  suspend fun getPaths(query: IndexQuery): Flow<String>

  suspend fun addTags(query: IndexQuery, tags: Collection<String>)

  suspend fun removeTags(query: IndexQuery, tags: Collection<String>)

  suspend fun setProperties(query: IndexQuery, properties: Map<String, Any>)

  suspend fun removeProperties(query: IndexQuery, properties: Collection<String>)

  suspend fun getPropertyValues(query: IndexQuery, propertyName: String): Flow<Any?>

  suspend fun getAttributeValues(query: IndexQuery, attributeName: String): Flow<Any?>

  suspend fun delete(query: IndexQuery)

  suspend fun delete(paths: Collection<String>)

  suspend fun getLayers(): Flow<String>

  suspend fun existsLayer(name: String): Boolean

  suspend fun setUpDatabaseIndexes(indexes: List<DatabaseIndex>)

}
