package io.georocket.query

/**
 * Compiles search strings to MongoDB queries
 * @since 1.0.0
 * @author Michel Kraemer
 */
interface QueryCompiler {
  /**
   * Specifies how the query returned by [compileQuery] should be applied
   */
  enum class MatchPriority {
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
     * [QueryCompiler] instances should be ignored.
     */
    ONLY
  }

  /**
   * Get the priority with which the MongoDB query returned by [compileQuery]
   * should be applied for the given GeoRocket query part
   */
  fun getQueryPriority(queryPart: QueryPart): MatchPriority

  /**
   * Create an index query for the given GeoRocket query part (may return
   * `null` if the given query part is unsupported)
   */
  fun compileQuery(queryPart: QueryPart): IndexQuery?
}
