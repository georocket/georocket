package io.georocket.index.postgresql

import io.vertx.core.json.JsonObject

object PostgreSQLQueryTranslator {
  fun translate(query: JsonObject): String {
    val result = StringBuilder()
    for (field in query.fieldNames()) {
      when (field) {
        "\$or" -> result.append("")
        else -> throw IllegalStateException("Cannot translate field `$field'")
      }
    }
    return result.toString()
  }
}
