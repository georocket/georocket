package io.georocket.query;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * An optimizer for Elasticsearch queries to make them more compact and
 * (probably) faster to evaluate.
 * @author Michel Kraemer
 * @since 1.1.0
 */
public class ElasticsearchQueryOptimizer {
  private ElasticsearchQueryOptimizer() {
    // hidden constructor
  }

  /**
   * <p>Optimize the given Elasticsearch query. For example, remove unnecessary
   * boolean clauses or merged nested ones.</p>
   * <p>Attention: This method is destructive. It changes the given object in
   * place.</p>
   * @param query the query to optimize
   * @return the optimized query (not necessarily the same object as
   * <code>query</code>)
   */
  public static JsonObject optimize(JsonObject query) {
    return (JsonObject)optimize(query, null);
  }

  /**
   * Optimize the given Elasticsearch query. The query may be embedded in
   * another one. In this case <code>parentClause</code> must be given. The
   * method can return a {@link JsonArray} instead of a {@link JsonObject} if
   * <code>parentClause</code> is not <code>null</code> and if the query should
   * be merged into the parent by the caller.
   * @param query the query to optimize
   * @param parentClause a parent clause such as "must", "should", "must_not"
   * @return the optimized query
   */
  private static Object optimize(JsonObject query, String parentClause) {
    if (query == null) {
      return null;
    }

    Object result = query;
    JsonObject bool = query.getJsonObject("bool");

    if (bool != null) {
      // optimize children
      Object should = bool.getValue("should");
      if (should != null) {
        bool.put("should", optimizeChildren(should, "should"));
      }
      Object must = bool.getValue("must");
      if (must != null) {
        bool.put("must", optimizeChildren(must, "must"));
      }
      Object must_not = bool.getValue("must_not");
      if (must_not != null) {
        bool.put("must_not", optimizeChildren(must_not, "must_not"));
      }

      // remove unnecessary "minimum_should_match" if there is only a "should"
      // clause and no "must" clause, or if there is no "should" clause at all
      if (bool.size() == 2) {
        Integer minimum_should_match = bool.getInteger("minimum_should_match");
        if (minimum_should_match != null && minimum_should_match == 1 &&
            (should != null || must != null || must_not != null)) {
          bool.remove("minimum_should_match");
        }
      }

      // remove bool if it has only one child
      if (bool.size() == 1) {
        String fieldName = bool.fieldNames().iterator().next();
        Object expr = bool.getValue(fieldName);
        if (expr instanceof JsonObject) {
          result = (JsonObject)expr;
        } else if (expr instanceof JsonArray) {
          // Merge array if there is only one item or if the current fieldName
          // matches parentClause. In the latter case the array will be merged
          // by the caller.
          JsonArray arrexpr = (JsonArray)expr;
          if (parentClause == null && arrexpr.size() == 1) {
            result = arrexpr.getJsonObject(0);
          } else if (parentClause != null && parentClause.equals(fieldName)) {
            result = expr;
          }
        }
      }
    }

    return result;
  }

  /**
   * Optimize a query or an array of queries
   * @param children a single query or an array of queries
   * @param parentClause the parent clause in which the children are embedded
   * @return the optimized query/queries
   */
  private static Object optimizeChildren(Object children, String parentClause) {
    if (children instanceof JsonArray) {
      return optimizeChildren((JsonArray)children, parentClause);
    } else {
      return optimize((JsonObject)children, parentClause);
    }
  }

  /**
   * Optimize an array of queries
   * @param children the queries to optimize
   * @param parentClause the parent clause in which the children are embedded
   * @return the optimized queries
   */
  private static JsonArray optimizeChildren(JsonArray children, String parentClause) {
    JsonArray copy = new JsonArray();
    for (int i = 0; i < children.size(); ++i) {
      Object c = children.getValue(i);
      Object newc = optimizeChildren(c, parentClause);
      if (newc instanceof JsonArray) {
        // merge optimized array into this one
        copy.addAll((JsonArray)newc);
      } else {
        copy.add(newc);
      }
    }
    return copy;
  }
}
