package io.georocket.index.xml

import io.georocket.index.Indexer
import io.georocket.index.IndexerFactory
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.QueryPart.ComparisonOperator
import io.georocket.query.StringQueryPart
import io.georocket.util.StreamEvent
import io.georocket.util.XMLStreamEvent
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Creates instances of [XalAddressIndexer]
 * @author Michel Kraemer
 */
class XalAddressIndexerFactory : IndexerFactory {
  override fun <T : StreamEvent> createIndexer(eventType: Class<T>): Indexer<T>? {
    if (eventType.isAssignableFrom(XMLStreamEvent::class.java)) {
      @Suppress("UNCHECKED_CAST")
      return XalAddressIndexer() as Indexer<T>
    }
    return null
  }

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return when (queryPart) {
      is StringQueryPart -> MatchPriority.SHOULD
      else -> MatchPriority.NONE
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject? {
    return when (queryPart) {
      is StringQueryPart -> {
        val v: Any = when (queryPart.comparisonOperator) {
          null, ComparisonOperator.EQ -> queryPart.value
          ComparisonOperator.GT -> jsonObjectOf("\$gt" to queryPart.value)
          ComparisonOperator.GTE -> jsonObjectOf("\$gte" to queryPart.value)
          ComparisonOperator.LT -> jsonObjectOf("\$lt" to queryPart.value)
          ComparisonOperator.LTE -> jsonObjectOf("\$lte" to queryPart.value)
        }

        if (queryPart.key == null) {
          jsonObjectOf("\$or" to XalAddressIndexer.Companion.Keys.values().map { key ->
            jsonObjectOf("address.${key.key}" to v)
          })
        } else if (XalAddressIndexer.Companion.Keys.values().map { it.key }.contains(queryPart.key)) {
          jsonObjectOf("address.${queryPart.key}" to v)
        } else {
          null
        }
      }

      else -> null
    }
  }
}