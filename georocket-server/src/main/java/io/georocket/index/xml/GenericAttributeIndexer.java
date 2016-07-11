package io.georocket.index.xml;

import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.ImmutableMap;

import io.georocket.util.XMLStreamEvent;

/**
 * Index for CityGML generic attributes
 * @author Michel
 */
public class GenericAttributeIndexer implements XMLIndexer {
  private static final String NS_GEN = "http://www.opengis.net/citygml/generics/2.0";
  
  /**
   * The key of the currently parsed generic attribute
   */
  private String currentKey = null;
  
  /**
   * True if we're currently parsing a value of a generic attribute
   */
  private boolean parsingValue = false;
  
  /**
   * Map collecting all attributes parsed
   */
  private Map<String, Object> result = new HashMap<>();
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    if ((event.getEvent() == XMLEvent.START_ELEMENT || event.getEvent() == XMLEvent.END_ELEMENT) &&
        event.getXMLReader().getNamespaceURI().equals(NS_GEN)) {
      String ln = event.getXMLReader().getLocalName();
      if (ln.equals("stringAttribute") || ln.equals("intAttribute") || ln.equals("doubleAttribute") ||
          ln.equals("dateAttribute") || ln.equals("uriAttribute") || ln.equals("measureAttribute")) {
        if (event.getEvent() == XMLEvent.START_ELEMENT) {
          currentKey = event.getXMLReader().getAttributeValue(null, "name");
          // Elasticsearch 2.x does not support dots in field names
          currentKey = currentKey.replace('.', '_');
        } else if (event.getEvent() == XMLEvent.END_ELEMENT) {
          currentKey = null;
        }
      } else if (ln.equals("value")) {
        if (event.getEvent() == XMLEvent.START_ELEMENT) {
          parsingValue = true;
        } else if (event.getEvent() == XMLEvent.END_ELEMENT) {
          parsingValue = false;
        }
      }
    } else if (event.getEvent() == XMLEvent.CHARACTERS && currentKey != null && parsingValue) {
      result.put(currentKey, event.getXMLReader().getText());
    }
  }

  @Override
  public Map<String, Object> getResult() {
    return ImmutableMap.of("genAttrs", result);
  }
}
