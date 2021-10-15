package io.georocket.index.generic

import io.georocket.index.IndexerFactory
import io.georocket.query.DoubleQueryPart
import io.georocket.query.LongQueryPart
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.QueryPart.ComparisonOperator
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject

/**
 * Base class for factories creating indexers that manage arbitrary generic
 * string attributes (i.e. key-value pairs)
 * @author Michel Kraemer
 */
abstract class GenericAttributeIndexerFactory : IndexerFactory {
  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return when (queryPart) {
      is StringQueryPart, is LongQueryPart, is DoubleQueryPart -> MatchPriority.SHOULD
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject? {
    return when (queryPart) {
      is StringQueryPart, is LongQueryPart, is DoubleQueryPart -> {
        val v = when (queryPart.comparisonOperator) {
          null, ComparisonOperator.EQ -> queryPart.value
          ComparisonOperator.GT -> mapOf("\$gt" to queryPart.value)
          ComparisonOperator.GTE -> mapOf("\$gte" to queryPart.value)
          ComparisonOperator.LT -> mapOf("\$lt" to queryPart.value)
          ComparisonOperator.LTE -> mapOf("\$lte" to queryPart.value)
        }

        val m = if (queryPart.key != null) {
          mapOf("key" to queryPart.key, "value" to v)
        } else {
          mapOf("value" to v)
        }

        // match values of all fields regardless of their name
        JsonObject(
          mapOf(
            "genAttrs" to mapOf(
              "\$elemMatch" to m
            )
          )
        )
      }
    }
  }
}
