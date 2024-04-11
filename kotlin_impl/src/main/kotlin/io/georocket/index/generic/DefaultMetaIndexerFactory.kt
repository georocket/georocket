package io.georocket.index.generic

import io.georocket.index.DatabaseIndex
import io.georocket.index.MetaIndexerFactory
import io.georocket.query.*
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart.ComparisonOperator.EQ

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

  override fun compileQuery(queryPart: QueryPart): IndexQuery {
    return when (queryPart) {
      is StringQueryPart, is LongQueryPart, is DoubleQueryPart -> {
        val key = queryPart.key
        if (key == null) {
          // match values of all fields regardless of their name
          val search = queryPart.value
          Or(
            Contains("tags", search),
            ElemMatchExists("props", "value" to search)
          )
        } else {
          val r = ElemMatchCompare(
            "props",
            listOf("key" to key),
            Compare("value", queryPart.value, queryPart.comparisonOperator ?: EQ),
          )
          if (queryPart.comparisonOperator == EQ && queryPart.key == "correlationId") {
            Or(r, Compare("correlationId", queryPart.value, EQ))
          } else {
            r
          }
        }
      }
    }
  }

  override fun getDatabaseIndexes(indexedFields: List<String>): List<DatabaseIndex> = listOf(
    DatabaseIndex.Array("tags", "tags_array"),
    DatabaseIndex.ElemMatchExists("props", listOf("value", "key"), "props_elem_match_exists"),
    *indexedFields.map { fieldName ->
      DatabaseIndex.ElemMatchCompare(
        "props",
        "key",
        "value",
        fieldName,
        "props_${fieldName}_elem_match_cmp"
      )
    }.toTypedArray(),
    DatabaseIndex.Eq("correlationId", "correlation_id_eq"),
    DatabaseIndex.StartsWith("layer", "layer_starts_with")
  )
}
