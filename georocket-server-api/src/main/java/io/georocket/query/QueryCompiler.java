package io.georocket.query;

import io.vertx.core.json.JsonObject;

/**
 * Compiles search strings to Elasticsearch documents
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
     * The query must appear in the matching documents
     */
    MUST,
    
    /**
     * The query is the only one that should be applied. Queries by other
     * {@link QueryCompiler} instances should be ignored.
     */
    ONLY
  }
  
  /**
   * Get the priority with which the query returned by {@link #compileQuery(String)}
   * should be applied for the given search string
   * @param search the search string
   * @return the priority
   */
  MatchPriority getQueryPriority(String search);
  
  /**
   * <p>Create an Elasticsearch query for the given search string.</p>
   * <p>Heads up: implementors may use the helper methods from
   * {@link ElasticsearchQueryHelper} to build the query.</p>
   * @param search the search string
   * @return the query (may be null)
   */
  JsonObject compileQuery(String search);

  /**
   * <p>Create an Elasticsearch query for the given search string.</p>
   * <p>Heads up: implementors may use the helper methods from
   * {@link ElasticsearchQueryHelper} to build the query.</p>
   * @param search the search string to compile
   * @param path the path where to perform the search (may be null if the
   * whole data store should be searched)
   * @return the query (may be null)
   */
  JsonObject compileQuery(String search, String path);
}
