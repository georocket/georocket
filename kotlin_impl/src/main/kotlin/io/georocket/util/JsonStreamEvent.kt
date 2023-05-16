package io.georocket.util

/**
 * An event produced during JSON parsing
 * @since 1.0.0
 * @author Michel Kraemer
 */
data class JsonStreamEvent(
  /**
   * the actual JSON event type
   */
  val event: Int,

  /**
   * the position in the JSON stream where the event has occurred
   */
  override val pos: Long,
  /**
   * the value or field name that was parsed when the event was
   * generated (may be `null`)
   */
  val currentValue: Any? = null
) : StreamEvent()
