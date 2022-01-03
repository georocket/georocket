package io.georocket.index.generic

import io.georocket.index.MetaIndexerFactory
import io.georocket.query.Compare
import io.georocket.query.Contains
import io.georocket.query.DoubleQueryPart
import io.georocket.query.ElemMatch
import io.georocket.query.IndexQuery
import io.georocket.query.LongQueryPart
import io.georocket.query.Or
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.QueryPart.ComparisonOperator.EQ
import io.georocket.query.StringQueryPart

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
            ElemMatch("props", Compare("value", search, EQ))
          )
        } else {
          val r = ElemMatch("props",
            Compare("key", key, EQ),
            Compare("value", queryPart.value, queryPart.comparisonOperator ?: EQ)
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
}
