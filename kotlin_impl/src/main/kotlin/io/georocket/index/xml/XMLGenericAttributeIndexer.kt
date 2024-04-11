package io.georocket.index.xml

import io.georocket.index.generic.GenericAttributeIndexer
import io.georocket.util.XMLStreamEvent
import javax.xml.stream.events.XMLEvent

/**
 * Indexer for CityGML generic attributes
 * @author Michel Kraemer
 */
class XMLGenericAttributeIndexer : GenericAttributeIndexer<XMLStreamEvent>() {
  /**
   * The key of the currently parsed generic attribute
   */
  private var currentKey: String? = null

  /**
   * True if we're currently parsing a value of a generic attribute
   */
  private var parsingValue = false

  override fun onEvent(event: XMLStreamEvent) {
    val e = event.event
    if (e == XMLEvent.START_ELEMENT || e == XMLEvent.END_ELEMENT) {
      val ln = event.xmlReader.localName
      if (ln == "stringAttribute" || ln == "intAttribute" || ln == "doubleAttribute" ||
          ln == "dateAttribute" || ln == "uriAttribute" || ln == "measureAttribute") {
        if (e == XMLEvent.START_ELEMENT) {
          currentKey = event.xmlReader.getAttributeValue(null, "name")
        } else if (e == XMLEvent.END_ELEMENT) {
          currentKey = null
        }
      } else if (ln == "value") {
        if (e == XMLEvent.START_ELEMENT) {
          parsingValue = true
        } else if (e == XMLEvent.END_ELEMENT) {
          parsingValue = false
        }
      }
    } else if (e == XMLEvent.CHARACTERS && currentKey != null && parsingValue) {
      put(currentKey!!, event.xmlReader.text)
    }
  }
}
