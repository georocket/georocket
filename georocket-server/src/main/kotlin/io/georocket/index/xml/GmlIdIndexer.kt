package io.georocket.index.xml

import io.georocket.util.XMLStreamEvent
import javax.xml.stream.events.XMLEvent

/**
 * Indexes GML IDs
 * @author Michel Kraemer
 */
class GmlIdIndexer : XMLIndexer {
  companion object {
    private const val NS_GML = "http://www.opengis.net/gml"
    private const val NS_GML_3_2 = "http://www.opengis.net/gml/3.2"
  }

  private var ids = mutableSetOf<String>()
  private var firstNsToCheck = NS_GML
  private var secondNsToCheck = NS_GML_3_2

  override fun onEvent(event: XMLStreamEvent) {
    if (event.event == XMLEvent.START_ELEMENT) {
      var gmlId = event.xmlReader.getAttributeValue(firstNsToCheck, "id")
      if (gmlId == null) {
        gmlId = event.xmlReader.getAttributeValue(secondNsToCheck, "id")
        if (gmlId != null) {
          // improve performance, by checking the second NS first
          val t = secondNsToCheck
          secondNsToCheck = firstNsToCheck
          firstNsToCheck = t
        }
      }
      if (gmlId != null) {
        ids.add(gmlId)
      }
    }
  }

  override fun getResult() = mapOf("gmlIds" to ids.toList())
}
