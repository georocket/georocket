package io.georocket.index.generic

import io.georocket.index.IndexerFactory
import io.georocket.query.KeyValueQueryPart
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject

/**
 * Base class for factories creating indexers that manage arbitrary generic
 * string attributes (i.e. key-value pairs)
 * @author Michel Kraemer
 */
abstract class GenericAttributeIndexerFactory : IndexerFactory {
  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return if (queryPart is StringQueryPart || queryPart is KeyValueQueryPart) {
      MatchPriority.SHOULD
    } else {
      MatchPriority.NONE
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject? {
    return when (queryPart) {
      is StringQueryPart -> {
        // match values of all fields regardless of their name
        val search = queryPart.searchString
        JsonObject(
          mapOf(
            "genAttrs" to mapOf(
              "\$elemMatch" to mapOf(
                "value" to search
              )
            )
          )
        )
      }

      is KeyValueQueryPart -> {
        val v: Any = when (queryPart.comparisonOperator) {
          ComparisonOperator.EQ -> queryPart.value
          ComparisonOperator.GT -> mapOf("\$gt" to queryPart.value)
          ComparisonOperator.GTE -> mapOf("\$gte" to queryPart.value)
          ComparisonOperator.LT -> mapOf("\$lt" to queryPart.value)
          ComparisonOperator.LTE -> mapOf("\$lte" to queryPart.value)
        }
        JsonObject(
          mapOf(
            "genAttrs" to mapOf(
              "\$elemMatch" to mapOf(
                "key" to queryPart.key,
                "value" to v
              )
            )
          )
        )
      }

      else -> null
    }
  }
}
