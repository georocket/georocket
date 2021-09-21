package io.georocket.index

import io.vertx.core.json.JsonObject

interface Index {
  suspend fun close()

  suspend fun add(id: String, doc: JsonObject)

  suspend fun addMany(docs: List<Pair<String, JsonObject>>)

  suspend fun getMeta(query: JsonObject): List<JsonObject>

  suspend fun delete(ids: List<String>)
}
