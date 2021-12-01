package io.georocket.index

import io.vertx.core.json.JsonObject

interface Index {
  suspend fun close()

  suspend fun add(id: String, doc: JsonObject)

  suspend fun addMany(docs: Collection<Pair<String, JsonObject>>)

  suspend fun getMeta(query: JsonObject): List<JsonObject>

  suspend fun addTags(query: JsonObject, tags: Collection<String>)

  suspend fun removeTags(query: JsonObject, tags: Collection<String>)

  suspend fun setProperties(query: JsonObject, properties: Map<String, Any>)

  suspend fun removeProperties(query: JsonObject, properties: Collection<String>)

  suspend fun getPropertyValues(query: JsonObject, propertyName: String): List<Any?>

  suspend fun getAttributeValues(query: JsonObject, attributeName: String): List<Any?>

  suspend fun delete(ids: Collection<String>)
}
