package io.georocket.query;

import java.util.Arrays;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Helper methods to build Elasticsearch queries
 * @author Michel Kraemer
 * @since 1.0.0
 */
public final class ElasticsearchQueryHelper {
  private ElasticsearchQueryHelper() {
    // hidden constructor
  }
  
  /**
   * Create a query that allows for boolean combinations of other queries
   * @return the query
   */
  public static JsonObject boolQuery() {
    return new JsonObject()
        .put("bool", new JsonObject());
  }
  
  /**
   * Create a query that allows for boolean combinations of other queries
   * @param minimumShouldMatch the minimum number of should clauses to match
   * @return the query
   * @since 1.1.0
   */
  public static JsonObject boolQuery(int minimumShouldMatch) {
    JsonObject r = boolQuery();
    r.getJsonObject("bool")
        .put("minimum_should_match", 1);
    return r;
  }
  
  /**
   * Add a filter to a bool query. Do not add it if the clause already exists
   * in the query.
   * @param bool the bool query
   * @param filter the filter to add
   */
  public static void boolAddFilter(JsonObject bool, JsonObject filter) {
    boolAdd(bool, filter, "filter");
  }
  
  /**
   * Add a "must" clause to a bool query. Do not add it if the clause already
   * exists in the query.
   * @param bool the bool query
   * @param must the clause to add
   */
  public static void boolAddMust(JsonObject bool, JsonObject must) {
    boolAdd(bool, must, "must");
  }
  
  /**
   * Add a "must_not" clause to a bool query. Do not add it if the clause
   * already exists in the query.
   * @param bool the bool query
   * @param mustNot the clause to add
   */
  public static void boolAddMustNot(JsonObject bool, JsonObject mustNot) {
    boolAdd(bool, mustNot, "must_not");
  }
  
  /**
   * Add a "should" clause to a bool query. Do not add it if the clause already
   * exists in the query.
   * @param bool the bool query
   * @param should the clause to add
   */
  public static void boolAddShould(JsonObject bool, JsonObject should) {
    boolAdd(bool, should, "should");
  }

  /**
   * Add a clause to a bool query. Do not add it if the clause already exists
   * in the query.
   * @param bool the bool query
   * @param clause the clause to add
   * @param occurrenceType the clause's occurrence type (e.g. "must", "should")
   */
  private static void boolAdd(JsonObject bool, JsonObject clause,
      String occurrenceType) {
    JsonObject b = bool.getJsonObject("bool");
    Object eso = b.getValue(occurrenceType);
    if (eso == null) {
      b.put(occurrenceType, clause);
    } else if (eso instanceof JsonArray) {
      JsonArray esoa = (JsonArray)eso;
      if (!esoa.contains(clause)) {
        esoa.add(clause);
      }
    } else {
      if (!eso.equals(clause)) {
        JsonArray es = new JsonArray().add(eso).add(clause);
        b.put(occurrenceType, es);
      }
    }
  }
  
  /**
   * Create a term query to exactly compare a field to a value
   * @param name the field's name
   * @param value the value to compare to
   * @return the term query
   */
  public static JsonObject termQuery(String name, Object value) {
    return new JsonObject()
        .put("term", new JsonObject()
            .put(name, value));
  }

  /**
   * Create a term query to compare a field to a value using less than
   * as operator.
   * @param name the field's name
   * @param value the value to compare to
   * @return the term query
   * @since 1.1.0
   */
  public static JsonObject ltQuery(String name, Object value) {
    return new JsonObject()
      .put("range", new JsonObject()
        .put(name, new JsonObject().put("lt", value)));
  }
  
  /**
   * Create a term query to compare a field to a value using less than
   * or equal to as operator.
   * @param name the field's name
   * @param value the value to compare to
   * @return the term query
   * @since 1.1.0
   */
  public static JsonObject lteQuery(String name, Object value) {
    return new JsonObject()
      .put("range", new JsonObject()
        .put(name, new JsonObject().put("lte", value)));
  }

  /**
   * Create a term query to compare a field to a value using greater
   * than as operator.
   * @param name the field's name
   * @param value the value to compare to
   * @return the term query
   * @since 1.1.0
   */
  public static JsonObject gtQuery(String name, Object value) {
    return new JsonObject()
      .put("range", new JsonObject()
        .put(name, new JsonObject().put("gt", value)));
  }

  /**
   * Create a term query to compare a field to a value using greater
   * than or equal to as operator.
   * @param name the field's name
   * @param value the value to compare to
   * @return the term query
   * @since 1.1.0
   */
  public static JsonObject gteQuery(String name, Object value) {
    return new JsonObject()
      .put("range", new JsonObject()
        .put(name, new JsonObject().put("gte", value)));
  }
  
  /**
   * Create a query that matches all documents
   * @return the query
   */
  public static JsonObject matchAllQuery() {
    return new JsonObject()
        .put("match_all", new JsonObject());
  }
  
  /**
   * Create a query that compares a value to multiple fields
   * @param text the value
   * @param fieldNames the names of the fields to compare to
   * @return the query
   */
  public static JsonObject multiMatchQuery(Object text, String... fieldNames) {
    return new JsonObject()
        .put("multi_match", new JsonObject()
            .put("query", text)
            .put("fields", new JsonArray(Arrays.asList(fieldNames))));
  }

  /**
   * Create a query that looks if a field with the given name exists
   * @param name the field's name
   * @return the query
   * @since 1.1.0
   */
  public static JsonObject existsQuery(String name) {
    return new JsonObject()
      .put("exists", new JsonObject()
        .put("field", name));
  }

  /**
   * Create a query that looks for a field starting with a given prefix
   * @param name the field's name
   * @param prefix the prefix
   * @return the query
   */
  public static JsonObject prefixQuery(String name, Object prefix) {
    return new JsonObject()
        .put("prefix", new JsonObject()
            .put(name, prefix));
  }

  /**
   * Create a query that looks for documents matching a geo shape
   * @param name the field name containing the document's geo shape
   * @param shape the geo shape to compare to
   * @param relation the relation used for comparison (i.e. "intersects",
   * "disjoint", "within", "contains")
   * @return the query
   */
  public static JsonObject geoShapeQuery(String name, JsonObject shape, String relation) {
    return new JsonObject()
        .put("geo_shape", new JsonObject()
            .put(name, new JsonObject()
                .put("shape", shape)
                .put("relation", relation)));
  }
  
  /**
   * Create a geo shape
   * @param type the shape's type (e.g. "envelope")
   * @param coordinates the shape's coordinates
   * @return the shape
   */
  public static JsonObject shape(String type, JsonArray coordinates) {
    return new JsonObject()
        .put("type", type)
        .put("coordinates", coordinates);
  }
}
