package io.georocket.index.xml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.ImmutableMap;

import io.georocket.util.XMLStreamEvent;

/**
 * Indexes GML IDs
 * @author Michel Kraemer
 */
public class GmlIdIndexer implements XMLIndexer {
  private static final String NS_GML = "http://www.opengis.net/gml";
  private static final String NS_GML_3_2 = "http://www.opengis.net/gml/3.2";
  
  protected Set<String> ids = new HashSet<>();

  private String firstNsToCheck = NS_GML;
  private String secondNsToCheck = NS_GML_3_2;
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    if (event.getEvent() == XMLEvent.START_ELEMENT) {
      String gmlId = event.getXMLReader().getAttributeValue(firstNsToCheck, "id");
      if (gmlId == null) {
        gmlId = event.getXMLReader().getAttributeValue(secondNsToCheck, "id");
        if (gmlId != null) {
          // improve performance, by checking the second NS first
          String t = secondNsToCheck;
          secondNsToCheck = firstNsToCheck;
          firstNsToCheck = t;
        }
      }
      if (gmlId != null) {
        ids.add(gmlId);
      }
    }
  }

  @Override
  public Map<String, Object> getResult() {
    return ImmutableMap.of("gmlIds", new ArrayList(ids));
  }
}
