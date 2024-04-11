package io.georocket.util

import javax.xml.stream.XMLStreamReader

/**
 * An event produced during XML parsing
 * @since 1.0.0
 * @author Michel Kraemer
 */
data class XMLStreamEvent(
  /**
   * The actual XML event type
   * @see javax.xml.stream.XMLStreamConstants
   */
  val event: Int,

  /**
   * the position in the XML stream where the event has occurred
   */
  override val pos: Long,

  /**
   * @return the XML reader that produced the event
   */
  val xmlReader: XMLStreamReader
) : StreamEvent()
