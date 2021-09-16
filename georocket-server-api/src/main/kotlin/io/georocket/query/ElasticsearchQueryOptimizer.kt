package io.georocket.query

import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray

/**
 * An optimizer for Elasticsearch queries to make them more compact and
 * (probably) faster to evaluate.
 * @author Michel Kraemer
 * @since 1.1.0
 */
object ElasticsearchQueryOptimizer {
  /**
   *
   * Optimize the given Elasticsearch query. For example, remove unnecessary
   * boolean clauses or merged nested ones.
   *
   * Attention: This method is destructive. It changes the given object in
   * place.
   * @param query the query to optimize
   * @return the optimized query (not necessarily the same object as
   * `query`)
   */
  fun optimize(query: JsonObject): JsonObject {
    return optimize(query, null) as JsonObject
  }

  /**
   * Optimize the given Elasticsearch query. The query may be embedded in
   * another one. In this case `parentClause` must be given. The
   * method can return a [JsonArray] instead of a [JsonObject] if
   * `parentClause` is not `null` and if the query should
   * be merged into the parent by the caller.
   * @param query the query to optimize
   * @param parentClause a parent clause such as "must", "should", "must_not"
   * @return the optimized query
   */
  private fun optimize(query: JsonObject, parentClause: String?): Any {
    var result: Any = query
    val bool = query.getJsonObject("bool")
    if (bool != null) {
      // optimize children
      val should = bool.getValue("should")
      if (should != null) {
        bool.put("should", optimizeChildren(should, "should"))
      }
      val must = bool.getValue("must")
      if (must != null) {
        bool.put("must", optimizeChildren(must, "must"))
      }
      val must_not = bool.getValue("must_not")
      if (must_not != null) {
        bool.put("must_not", optimizeChildren(must_not, "must_not"))
      }

      // remove unnecessary "minimum_should_match" if there is only a "should"
      // clause and no "must" clause, or if there is no "should" clause at all
      if (bool.size() == 2) {
        val minimum_should_match = bool.getInteger("minimum_should_match")
        if (minimum_should_match != null && minimum_should_match == 1 &&
          (should != null || must != null || must_not != null)
        ) {
          bool.remove("minimum_should_match")
        }
      }

      // remove bool if it has only one child - but keep must_not
      if (bool.size() == 1) {
        val fieldName = bool.fieldNames().iterator().next()
        if ("must_not" != fieldName) {
          val expr = bool.getValue(fieldName)
          if (expr is JsonObject) {
            result = expr
          } else if (expr is JsonArray) {
            // Merge array if there is only one item or if the current fieldName
            // matches parentClause. In the latter case the array will be merged
            // by the caller.
            val arrexpr = expr
            if (parentClause == null && arrexpr.size() == 1) {
              result = arrexpr.getJsonObject(0)
            } else if (parentClause != null && parentClause == fieldName) {
              result = expr
            }
          }
        }
      }
    }
    return result
  }

  /**
   * Optimize a query or an array of queries
   * @param children a single query or an array of queries
   * @param parentClause the parent clause in which the children are embedded
   * @return the optimized query/queries
   */
  private fun optimizeChildren(children: Any, parentClause: String): Any? {
    return if (children is JsonArray) {
      optimizeChildren(
        children,
        parentClause
      )
    } else {
      optimize(children as JsonObject, parentClause)
    }
  }

  /**
   * Optimize an array of queries
   * @param children the queries to optimize
   * @param parentClause the parent clause in which the children are embedded
   * @return the optimized queries
   */
  private fun optimizeChildren(
    children: JsonArray,
    parentClause: String
  ): JsonArray {
    val copy = JsonArray()
    for (i in 0 until children.size()) {
      val c = children.getValue(i)
      val newc = optimizeChildren(c, parentClause)
      if (newc is JsonArray) {
        // merge optimized array into this one
        copy.addAll(newc as JsonArray?)
      } else {
        copy.add(newc)
      }
    }
    return copy
  }
}