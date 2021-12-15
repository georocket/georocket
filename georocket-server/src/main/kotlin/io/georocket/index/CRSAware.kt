package io.georocket.index

/**
 * Indexers implementing this interface are aware of coordinate reference systems
 * @since 1.0.0
 * @author Michel Kraemer
 */
interface CRSAware {
  /**
   * Set a string representing the [crs] that should be used to index a chunk if
   * it does not specify a CRS itself (may be `null` if no CRS is available as
   * fallback)
   */
  fun setFallbackCRSString(crs: String?)
}
