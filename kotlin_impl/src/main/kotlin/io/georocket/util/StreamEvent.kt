package io.georocket.util

/**
 * An event produced during input parsing
 * @since 1.0.0
 * @author Michel Kraemer
 */
abstract class StreamEvent {
  /**
   * the position in the input stream where the event has occurred
   */
  abstract val pos: Long
}
