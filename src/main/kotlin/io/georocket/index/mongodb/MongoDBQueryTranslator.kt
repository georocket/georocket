package io.georocket.index.mongodb

import io.georocket.query.All
import io.georocket.query.And
import io.georocket.query.Compare
import io.georocket.query.Contains
import io.georocket.query.ElemMatch
import io.georocket.query.GeoIntersects
import io.georocket.query.IndexQuery
import io.georocket.query.Not
import io.georocket.query.Or
import io.georocket.query.QueryPart.ComparisonOperator.EQ
import io.georocket.query.QueryPart.ComparisonOperator.GT
import io.georocket.query.QueryPart.ComparisonOperator.GTE
import io.georocket.query.QueryPart.ComparisonOperator.LT
import io.georocket.query.QueryPart.ComparisonOperator.LTE
import io.georocket.query.StartsWith
import io.georocket.query.escapeRegex
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

      is ElemMatch -> {
        if (query.operands.size == 1) {
          val fo = query.operands.first()
          val tfo = translate(fo).map.entries.first()
          jsonObjectOf("${query.arrayField}.${tfo.key}" to tfo.value)
        } else {
          val ops = mutableMapOf<String, Any>()
          for (op in query.operands) {
            ops.putAll(translate(op).map)
          }
          jsonObjectOf(query.arrayField to jsonObjectOf("\$elemMatch" to ops))
        }
      }

      is GeoIntersects -> jsonObjectOf(query.field to jsonObjectOf("\$geoIntersects" to
          jsonObjectOf("\$geometry" to query.geometry)))

      is Not -> jsonObjectOf("\$not" to translate(query.operand))

      is Or -> jsonObjectOf("\$or" to JsonArray(query.operands.map { translate(it) }))

      is StartsWith -> jsonObjectOf(query.field to jsonObjectOf("\$regex" to "^${query.prefix.escapeRegex()}"))
    }
  }
}
