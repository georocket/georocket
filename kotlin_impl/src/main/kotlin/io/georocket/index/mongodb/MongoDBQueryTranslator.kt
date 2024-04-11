package io.georocket.index.mongodb

import io.georocket.query.*
import io.georocket.query.QueryPart.ComparisonOperator.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Translates [IndexQuery] objects to MongoDB queries
 * @author Michel Kraemer
 */
object MongoDBQueryTranslator {
  /**
   * Translate an index [query] to a MongoDB query
   */
  fun translate(query: IndexQuery): JsonObject {
    return when (query) {
      is All -> jsonObjectOf()

      is And -> jsonObjectOf("\$and" to JsonArray(query.operands.map { translate(it) }))

      is Compare -> {
        when (query.op) {
          EQ -> jsonObjectOf(query.field to query.value)
          GT -> jsonObjectOf(query.field to jsonObjectOf("\$gt" to query.value))
          GTE -> jsonObjectOf(query.field to jsonObjectOf("\$gte" to query.value))
          LT -> jsonObjectOf(query.field to jsonObjectOf("\$lt" to query.value))
          LTE -> jsonObjectOf(query.field to jsonObjectOf("\$lte" to query.value))
        }
      }

      is Contains -> jsonObjectOf(query.arrayField to query.value)

      is ElemMatchExists -> {
        if (query.matches.size == 1) {
          val (field, value) = query.matches.first()
          val tfo = translate(Compare(field, value, EQ)).map.entries.first()
          jsonObjectOf("${query.arrayField}.${tfo.key}" to tfo.value)
        } else {
          val ops = mutableMapOf<String, Any>()
          for ((field, value) in query.matches) {
            ops.putAll(translate(Compare(field, value, EQ)).map)
          }
          jsonObjectOf(query.arrayField to jsonObjectOf("\$elemMatch" to query))
        }
      }

      is GeoIntersects -> jsonObjectOf(
        query.field to jsonObjectOf(
          "\$geoIntersects" to
            jsonObjectOf("\$geometry" to query.geometry)
        )
      )

      is Not -> jsonObjectOf("\$not" to translate(query.operand))

      is Or -> jsonObjectOf("\$or" to JsonArray(query.operands.map { translate(it) }))

      is StartsWith -> jsonObjectOf(query.field to jsonObjectOf("\$regex" to "^${query.prefix.escapeRegex()}"))
      is ElemMatchCompare -> {
        val ops = mutableMapOf<String, Any>()
        query.match.forEach { (field, value) ->
          ops.putAll(translate(Compare(field, value, EQ)).map)
        }
        ops.putAll(translate(query.condition).map)
        jsonObjectOf(
          query.arrayField to jsonObjectOf(
            "\$elemMatch" to ops
          )
        )
      }
    }
  }
}
