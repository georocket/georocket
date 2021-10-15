package io.georocket.index.generic

import io.georocket.index.MetaIndexerFactory
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
      is StringQueryPart -> MatchPriority.SHOULD
      else -> MatchPriority.NONE
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject? {
    return when (queryPart) {
      // TODO support LongQueryPart and DoubleQueryPart too
      is StringQueryPart -> {
        if (queryPart.key == null) {
          // match values of all fields regardless of their name
          val search = queryPart.value
          jsonObjectOf("tags" to search)
        } else {
          val v: Any = when (queryPart.comparisonOperator) {
            null, ComparisonOperator.EQ -> queryPart.value
            ComparisonOperator.GT -> jsonObjectOf("\$gt" to queryPart.value)
            ComparisonOperator.GTE -> jsonObjectOf("\$gte" to queryPart.value)
            ComparisonOperator.LT -> jsonObjectOf("\$lt" to queryPart.value)
            ComparisonOperator.LTE -> jsonObjectOf("\$lte" to queryPart.value)
          }

          val name = "props.${queryPart.key}"
          val r = jsonObjectOf(name to v)

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

      else  -> null
    }
  }
}
