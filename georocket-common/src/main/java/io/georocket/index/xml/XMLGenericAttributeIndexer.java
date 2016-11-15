package io.georocket.index.xml;

import javax.xml.stream.events.XMLEvent;

import io.georocket.index.generic.GenericAttributeIndexer;
import io.georocket.util.XMLStreamEvent;

/**
 * Indexer for CityGML generic attributes
 * @author Michel Kraemer
 */
public class XMLGenericAttributeIndexer extends GenericAttributeIndexer
    implements XMLIndexer {
  /**
   * The key of the currently parsed generic attribute
   */
  private String currentKey = null;
  
  /**
   * True if we're currently parsing a value of a generic attribute
   */
  private boolean parsingValue = false;
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    int e = event.getEvent();
    if ((e == XMLEvent.START_ELEMENT || e == XMLEvent.END_ELEMENT)) {
      String ln = event.getXMLReader().getLocalName();
      if (ln.equals("stringAttribute") || ln.equals("intAttribute") || ln.equals("doubleAttribute") ||
          ln.equals("dateAttribute") || ln.equals("uriAttribute") || ln.equals("measureAttribute")) {
        if (e == XMLEvent.START_ELEMENT) {
          currentKey = event.getXMLReader().getAttributeValue(null, "name");
        } else if (e == XMLEvent.END_ELEMENT) {
          currentKey = null;
        }
      } else if (ln.equals("value")) {
        if (e == XMLEvent.START_ELEMENT) {
          parsingValue = true;
        } else if (e == XMLEvent.END_ELEMENT) {
          parsingValue = false;
        }
      }
    } else if (e == XMLEvent.CHARACTERS && currentKey != null && parsingValue) {
      put(currentKey, event.getXMLReader().getText());
    }
  }
}
