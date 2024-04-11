package io.georocket.index.generic

import io.georocket.index.DatabaseIndex
import io.georocket.index.Indexer
import io.georocket.index.IndexerFactory
import io.georocket.index.geojson.GeoJsonGenericAttributeIndexer
import io.georocket.index.xml.XMLGenericAttributeIndexer
import io.georocket.query.*
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart.ComparisonOperator.EQ
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
          ElemMatchCompare(
            "genAttrs",
            "key" to key,
            Compare("value", queryPart.value, queryPart.comparisonOperator ?: EQ)
          )
        } else {
          ElemMatchExists(
            "genAttrs",
            "value" to queryPart.value
          )
        }
      }
    }
  }

  override fun getDatabaseIndexes(indexedFields: List<String>): List<DatabaseIndex> = listOf(
    DatabaseIndex.ElemMatchExists("genAttrs", listOf("value", "key"), "gen_attrs_elem_match_exists"),
    *indexedFields.map { fieldName ->
      DatabaseIndex.ElemMatchCompare(
        "genAttrs",
        "key",
        "value",
        fieldName,
        "gen_attrs_${fieldName}_elem_match_cmp"
      )
    }.toTypedArray(),
  )
}
