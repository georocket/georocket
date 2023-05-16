package io.georocket.util

/**
 * Generates ID
 * @author Michel Kraemer
 */
interface IDGenerator {
  /**
   * Generate a ID
   * @return the ID
   */
  fun next(): String
}
