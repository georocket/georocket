package io.georocket.query;

import io.vertx.core.json.JsonObject;

/**
 * Compiles search strings to MongoDB queries
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface QueryCompiler {
  /**
   * Specifies how the query returned by {@link QueryCompiler#compileQuery(String)}
   * should be applied
   */
  enum MatchPriority {
    /**
     * The query should not be applied at all
     */
    NONE,
    
    /**
     * The query should appear in the matching documents
     */
    SHOULD,
    
    /**
     * The query is the only one that should be applied. Queries by other
     * {@link QueryCompiler} instances should be ignored.
     */
    ONLY
  }
  
  /**
   * Get the priority with which the Elasticsearch query returned by
   * {@link #compileQuery(String)} should be applied for the given search string
   * @param search the search string
   * @return the priority
   */
  default MatchPriority getQueryPriority(String search) {
    return MatchPriority.NONE;
  }
  
  /**
   * Get the priority with which the Elasticsearch query returned by
   * {@link #compileQuery(QueryPart)} should be applied for the given
   * GeoRocket query part
   * @param queryPart the GeoRocket query part
   * @return the priority
   * @since 1.1.0
   */
  default MatchPriority getQueryPriority(QueryPart queryPart) {
    if (queryPart instanceof StringQueryPart) {
      return getQueryPriority(((StringQueryPart)queryPart).getSearchString());
    }
    return MatchPriority.NONE;
  }
  
  /**
   * <p>Create a MongoDB query for the given search string.</p>
   * @param search the search string
   * @return the Elasticsearch query (may be null)
   */
  default JsonObject compileQuery(String search) {
    return null;
  }
  
  /**
   * <p>Create an Elasticsearch query for the given GeoRocket query part.</p>
   * @param queryPart the GeoRocket query part
   * @return the Elasticsearch query (may be null)
   * @since 1.1.0
   */
  default JsonObject compileQuery(QueryPart queryPart) {
    if (queryPart instanceof StringQueryPart) {
      return compileQuery(((StringQueryPart)queryPart).getSearchString());
    }
    return null;
  }
}
