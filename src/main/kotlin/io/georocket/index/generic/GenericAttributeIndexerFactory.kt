package io.georocket.index.generic

import io.georocket.index.Indexer
import io.georocket.index.IndexerFactory
import io.georocket.index.geojson.GeoJsonGenericAttributeIndexer
import io.georocket.index.xml.XMLGenericAttributeIndexer
import io.georocket.query.Compare
import io.georocket.query.DoubleQueryPart
import io.georocket.query.ElemMatch
import io.georocket.query.IndexQuery
import io.georocket.query.LongQueryPart
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.QueryPart.ComparisonOperator
import io.georocket.query.QueryPart.ComparisonOperator.EQ
import io.georocket.query.StringQueryPart
import io.georocket.util.JsonStreamEvent
import io.georocket.util.StreamEvent
import io.georocket.util.XMLStreamEvent

/**
 * Base class for factories creating indexers that manage arbitrary generic
 * string attributes (i.e. key-value pairs)
 * @author Michel Kraemer
 */
class GenericAttributeIndexerFactory : IndexerFactory {
  @Suppress("UNCHECKED_CAST")
  override fun <T : StreamEvent> createIndexer(eventType: Class<T>): Indexer<T>? {
    if (eventType.isAssignableFrom(XMLStreamEvent::class.java)) {
      return XMLGenericAttributeIndexer() as Indexer<T>
    } else if (eventType.isAssignableFrom(JsonStreamEvent::class.java)) {
      return GeoJsonGenericAttributeIndexer() as Indexer<T>
    }
    return null
  }

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return when (queryPart) {
      is StringQueryPart, is LongQueryPart, is DoubleQueryPart -> MatchPriority.SHOULD
    }
  }

  override fun compileQuery(queryPart: QueryPart): IndexQuery {
    return when (queryPart) {
      is StringQueryPart, is LongQueryPart, is DoubleQueryPart -> {
        val key = queryPart.key
        if (key != null) {
          ElemMatch("genAttrs",
            Compare("key", key, EQ),
            Compare("value", queryPart.value, queryPart.comparisonOperator ?: EQ)
          )
        } else {
          ElemMatch("genAttrs",
            Compare("value", queryPart.value, EQ)
          )
        }
      }
    }
  }
}
