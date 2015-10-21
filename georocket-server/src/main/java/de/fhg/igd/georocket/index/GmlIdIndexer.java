package de.fhg.igd.georocket.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.ImmutableMap;

import de.fhg.igd.georocket.util.XMLStreamEvent;

/**
 * Indexes GML IDs
 * @author Michel Kraemer
 */
public class GmlIdIndexer implements Indexer {
  private static final String NS_GML = "http://www.opengis.net/gml";
  
  private List<String> ids = new ArrayList<>();
  
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
