package io.georocket.index.xml;

import java.util.HashSet;
import java.util.Map;

import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.ImmutableMap;

import io.georocket.util.XMLStreamEvent;

/**
 * Indexes GML IDs
 * @author Michel Kraemer
 */
public class GmlIdIndexer implements XMLIndexer {
  private static final String NS_GML = "http://www.opengis.net/gml";
  
  protected HashSet<String> ids = new HashSet<>();
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    if (event.getEvent() == XMLEvent.START_ELEMENT) {
      String gmlId = event.getXMLReader().getAttributeValue(NS_GML, "id");
      if (gmlId != null) {
        ids.add(gmlId);
      }
    }
  }

  @Override
  public Map<String, Object> getResult() {
    return ImmutableMap.of("gmlIds", ids);
  }
}
