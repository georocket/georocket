package io.georocket.storage

/**
 * Metadata affecting the way a chunk is indexed
 * @author Michel Kraemer
 */
data class IndexMeta(
  /**
   * An ID that specifies to which specific import this metadata belongs
   */
  val correlationId: String,

  /**
   * The name of the source file containing the chunks to be indexed
   */
  val filename: String,

  /**
   * The timestamp for this import
   */
  val timestamp: Long,

  /**
   * The layer where the file should be stored
   */
  val layer: String,

  /**
   * The list of tags to attach to the chunk
   */
  val tags: List<String>? = null,

  /**
   * The map of properties to attach to the chunk
   */
  val properties: Map<String, Any>? = null,

  /**
   * A string representing the CRS that should be used to index the chunk to
   * import if it does not specify a CRS itself (may be `nul if no CRS is
   * available as fallback)
   */
  val fallbackCRSString: String? = null,
)
