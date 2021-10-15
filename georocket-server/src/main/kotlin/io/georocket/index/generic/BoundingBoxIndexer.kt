package io.georocket.index.generic

import io.georocket.index.Indexer
import io.georocket.util.StreamEvent
import io.vertx.core.logging.LoggerFactory

/**
 * Base class for all indexers that create Bounding Boxes
 * @author Michel Kraemer
 */
abstract class BoundingBoxIndexer<T : StreamEvent> : Indexer<T> {
  companion object {
    private val log = LoggerFactory.getLogger(BoundingBoxIndexer::class.java)
  }

  /**
   * `true` if [addToBoundingBox] has been called at least once
   */
  private var boundingBoxInitialized = false

  /**
   * The calculated bounding box of this chunk. Only contains valid values
   * if [boundingBoxInitialized] is `true`
   */
  private var minX = 0.0
  private var maxX = 0.0
  private var minY = 0.0
  private var maxY = 0.0

  /**
   * Adds the given coordinate to the bounding box
   * @param x the x ordinate
   * @param y the y ordinate
   */
  protected fun addToBoundingBox(x: Double, y: Double) {
    if (!boundingBoxInitialized) {
      maxX = x
      minX = maxX
      maxY = y
      minY = maxY
      boundingBoxInitialized = true
    } else {
      if (x < minX) {
        minX = x
      }
      if (x > maxX) {
        maxX = x
      }
      if (y < minY) {
        minY = y
      }
      if (y > maxY) {
        maxY = y
      }
    }
  }

  /**
   * Check if the current bounding box contains valid values for WGS84
   */
  private fun validate(): Boolean {
    return minX >= -180.0 && minY >= -90.0 && maxX <= 180.0 && maxY <= 90.0
  }

  override fun makeResult(): Map<String, Any> {
    if (!boundingBoxInitialized) {
      // the chunk's bounding box is unknown. do not add it to the index
      return emptyMap()
    }

    if (!validate()) {
      boundingBoxInitialized = false
      log.warn("Invalid bounding box [$minX, $minY, $maxX, $maxY]. Values " +
          "outside [-180.0, -90.0, 180.0, 90.0]. Skipping chunk.")
      return emptyMap()
    }

    return mapOf(
      "bbox" to mapOf(
        "type" to "Polygon",
        "coordinates" to listOf(
          listOf(
            listOf(minX, minY),
            listOf(maxX, minY),
            listOf(maxX, maxY),
            listOf(minX, maxY),
            listOf(minX, minY)
          )
        )
      )
    )
  }
}
