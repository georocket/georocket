package io.georocket.index.xml;

import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.ImmutableMap;

import io.georocket.util.XMLStreamEvent;

/**
 * Index for CityGML generic attributes
 * @author Michel Kraemer
 */
public class GenericAttributeIndexer implements XMLIndexer {
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
  protected Map<String, String> result = new HashMap<>();
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    if ((event.getEvent() == XMLEvent.START_ELEMENT || event.getEvent() == XMLEvent.END_ELEMENT)) {
      String ln = event.getXMLReader().getLocalName();
      if (ln.equals("stringAttribute") || ln.equals("intAttribute") || ln.equals("doubleAttribute") ||
          ln.equals("dateAttribute") || ln.equals("uriAttribute") || ln.equals("measureAttribute")) {
        if (event.getEvent() == XMLEvent.START_ELEMENT) {
          currentKey = event.getXMLReader().getAttributeValue(null, "name");
          // Remove dot from field name so that Elasticsearch does not think
          // this field is of type 'object' instead of 'keyword'
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
