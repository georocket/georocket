package io.georocket.index.generic

import io.georocket.index.MetaIndexerFactory
import io.georocket.query.DoubleQueryPart
import io.georocket.query.LongQueryPart
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.QueryPart.ComparisonOperator
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Factory for [DefaultMetaIndexer] instances
 * @author Michel Kraemer
 */
class DefaultMetaIndexerFactory : MetaIndexerFactory {
  override fun createIndexer() = DefaultMetaIndexer()

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return when (queryPart) {
      is StringQueryPart, is LongQueryPart, is DoubleQueryPart -> MatchPriority.SHOULD
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject {
    return when (queryPart) {
      is StringQueryPart, is LongQueryPart, is DoubleQueryPart -> {
        if (queryPart.key == null) {
          // match values of all fields regardless of their name
          val search = queryPart.value
          jsonObjectOf("\$or" to jsonArrayOf(
            jsonObjectOf("tags" to search),
            jsonObjectOf("props.value" to search)
          ))
        } else {
          val v: Any = when (queryPart.comparisonOperator) {
            null, ComparisonOperator.EQ -> queryPart.value
            ComparisonOperator.GT -> jsonObjectOf("\$gt" to queryPart.value)
            ComparisonOperator.GTE -> jsonObjectOf("\$gte" to queryPart.value)
            ComparisonOperator.LT -> jsonObjectOf("\$lt" to queryPart.value)
            ComparisonOperator.LTE -> jsonObjectOf("\$lte" to queryPart.value)
          }

          val m = if (queryPart.key != null) {
            mapOf("key" to queryPart.key, "value" to v)
          } else {
            mapOf("value" to v)
          }

          val r = jsonObjectOf("props" to jsonObjectOf(
            "\$elemMatch" to m
          ))

          if (queryPart.comparisonOperator == ComparisonOperator.EQ && queryPart.key == "correlationId") {
            jsonObjectOf(
              "\$or" to jsonArrayOf(
                r,
                jsonObjectOf("correlationId" to queryPart.value)
              )
            )
          } else {
            r
          }
        }
      }
    }
  }
}
