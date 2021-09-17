package io.georocket.index.xml

import io.georocket.query.KeyValueQueryPart
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Creates instances of [XalAddressIndexer]
 * @author Michel Kraemer
 */
class XalAddressIndexerFactory : XMLIndexerFactory {
  override fun createIndexer() = XalAddressIndexer()

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
        jsonObjectOf("\$or" to XalAddressIndexer.Companion.Keys.values().map { key ->
          jsonObjectOf("address.${key.key}" to queryPart.searchString)
        })
      }

      is KeyValueQueryPart -> {
        val name = "address.${queryPart.key}"
        when (queryPart.comparisonOperator) {
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
      }

      else -> null
    }
  }
}
