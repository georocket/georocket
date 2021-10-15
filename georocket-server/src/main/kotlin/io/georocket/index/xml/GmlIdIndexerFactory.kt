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
 * Create instances of [GmlIdIndexer]
 * @author Michel Kraemer
 */
class GmlIdIndexerFactory : IndexerFactory {
  override fun <T : StreamEvent> createIndexer(eventType: Class<T>): Indexer<T>? {
    if (eventType.isAssignableFrom(XMLStreamEvent::class.java)) {
      @Suppress("UNCHECKED_CAST")
      return GmlIdIndexer() as Indexer<T>
    }
    return null
  }

  /**
   * Test if the given key-value query part refers to a gmlId and if it uses
   * the EQ operator (e.g. EQ(gmlId myId) or EQ(gml:id myId))
   */
  private fun isGmlIdEQ(qp: StringQueryPart): Boolean {
    val key = qp.key
    val comp = qp.comparisonOperator
    return comp === ComparisonOperator.EQ && ("gmlId" == key || "gml:id" == key)
  }

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return if (queryPart is StringQueryPart) {
      if (queryPart.key == null || isGmlIdEQ(queryPart)) {
        return MatchPriority.SHOULD
      } else {
        MatchPriority.NONE
      }
    } else {
      MatchPriority.NONE
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject? {
    return if (queryPart is StringQueryPart) {
      if (queryPart.key == null || isGmlIdEQ(queryPart)) {
        jsonObjectOf("gmlIds" to queryPart.value)
      } else {
        null
      }
    } else {
      null
    }
  }
}
