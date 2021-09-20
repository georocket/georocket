package io.georocket.index.xml

import io.georocket.query.KeyValueQueryPart
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Create instances of [GmlIdIndexer]
 * @author Michel Kraemer
 */
class GmlIdIndexerFactory : XMLIndexerFactory {
  override fun createIndexer() = GmlIdIndexer()

  /**
   * Test if the given key-value query part refers to a gmlId and if it uses
   * the EQ operator (e.g. EQ(gmlId myId) or EQ(gml:id myId))
   */
  private fun isGmlIdEQ(kvqp: KeyValueQueryPart): Boolean {
    val key = kvqp.key
    val comp = kvqp.comparisonOperator
    return comp === ComparisonOperator.EQ && ("gmlId" == key || "gml:id" == key)
  }

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return if (queryPart is StringQueryPart) {
      MatchPriority.SHOULD
    } else if (queryPart is KeyValueQueryPart && isGmlIdEQ(queryPart)) {
      MatchPriority.SHOULD
    } else {
      MatchPriority.NONE
    }
  }

  override fun compileQuery(queryPart: QueryPart): JsonObject? {
    return if (queryPart is StringQueryPart) {
      jsonObjectOf("gmlIds" to queryPart.searchString)
    } else if (queryPart is KeyValueQueryPart && isGmlIdEQ(queryPart)) {
      jsonObjectOf("gmlIds" to queryPart.value)
    } else {
      null
    }
  }
}
