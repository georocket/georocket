package io.georocket.api.index;

import org.elasticsearch.index.query.QueryBuilder;

/**
 * Compiles search strings to Elasticsearch documents
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
   * Get the priority with which the query returned by {@link #makeQuery(String)}
   * should be applied for the given search string
   * @param search the search string
   * @return the priority
   */
  MatchPriority getQueryPriority(String search);
  
  /**
   * Create an Elasticsearch query for the given search string
   * @param search the search string
   * @return the query (may be null)
   */
  QueryBuilder compileQuery(String search);
}
