package io.georocket.api.index;

import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;

/**
 * Factory for {@link Indexer} objects
 * @author Michel Kraemer
 */
public interface IndexerFactory {
  /**
   * Specifies how the query returned by {@link IndexerFactory#makeQuery(String)}
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
     * {@link IndexerFactory} instances should be ignored.
     */
    ONLY
  }
  
  /**
   * @return the Elasticsearch mapping for the results returned by the indexer
   */
  Map<String, Object> getMapping();
  
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
  QueryBuilder makeQuery(String search);
  
  /**
   * @return a new instance of {@link Indexer}
   */
  Indexer createIndexer();
}
