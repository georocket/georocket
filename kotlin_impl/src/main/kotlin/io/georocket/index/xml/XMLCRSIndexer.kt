package io.georocket.index.xml

import io.georocket.index.Indexer
import io.georocket.util.XMLStreamEvent
import javax.xml.stream.events.XMLEvent

/**
 * Indexes the coordinate reference system of a chunk. Only indexes the first
 * CRS found.
 * @author Michel Kraemer
 */
class XMLCRSIndexer : Indexer<XMLStreamEvent> {
  /**
   * The string of the detected CRS
   */
  var crs: String? = null
    private set

  override fun onEvent(event: XMLStreamEvent) {
    if (crs != null) {
      // we already found a CRS
      return
    }
    val reader = event.xmlReader
    if (event.event == XMLEvent.START_ELEMENT) {
      crs = reader.getAttributeValue(null, "srsName")
    }
  }

  override fun makeResult() = crs?.let { mapOf<String, Any>("crs" to it) } ?: emptyMap()
}
