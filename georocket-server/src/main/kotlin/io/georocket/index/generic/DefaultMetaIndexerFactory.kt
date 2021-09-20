package io.georocket.index.generic

import io.georocket.index.xml.MetaIndexer
import io.georocket.index.xml.MetaIndexerFactory
import io.georocket.query.KeyValueQueryPart
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Factory for [DefaultMetaIndexer] instances. Contains default mappings
 * required for essential GeoRocket indexer operations.
 * @author Michel Kraemer
 */
class DefaultMetaIndexerFactory : MetaIndexerFactory {
  override fun createIndexer() = DefaultMetaIndexer()

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return if (queryPart is StringQueryPart || queryPart is KeyValueQueryPart) {
      MatchPriority.SHOULD
    } else {
      MatchPriority.NONE
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject? {
    val result = if (queryPart is StringQueryPart) {
      // match values of all fields regardless of their name
      val search = queryPart.searchString
      jsonObjectOf("tags" to search)
    } else if (queryPart is KeyValueQueryPart) {
      val name = "props.${queryPart.key}"
      val r = when (queryPart.comparisonOperator) {
        ComparisonOperator.EQ -> jsonObjectOf(
          name to queryPart.value
        )

        ComparisonOperator.GT -> jsonObjectOf(
          name to jsonObjectOf(
            "\$gt" to queryPart.value
          )
        )

        ComparisonOperator.GTE -> jsonObjectOf(
          name to jsonObjectOf(
            "\$gte" to queryPart.value
          )
        )

        ComparisonOperator.LT -> jsonObjectOf(
          name to jsonObjectOf(
            "\$lt" to queryPart.value
          )
        )

        ComparisonOperator.LTE -> jsonObjectOf(
          name to jsonObjectOf(
            "\$lte" to queryPart.value
          )
        )
      }

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
    } else {
      null
    }
    return result
  }
}
