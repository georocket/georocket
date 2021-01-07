package io.georocket.index

import io.vertx.core.json.JsonObject

interface Index {
  suspend fun close()

  suspend fun add(id: String, doc: JsonObject)
}
