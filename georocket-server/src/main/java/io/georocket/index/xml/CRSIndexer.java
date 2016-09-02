package io.georocket.index.xml;

import java.util.Map;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.ImmutableMap;

import io.georocket.util.XMLStreamEvent;

/**
 * Indexes the coordinate reference system of a chunk. Only indexes first
 * CRS found.
 * @author Michel Kraemer
 */
public class CRSIndexer implements XMLIndexer {
  /**
   * The string of the detected CRS
   */
  protected String crsStr;
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    if (crsStr != null) {
      // we already found a CRS
      return;
    }
    
    XMLStreamReader reader = event.getXMLReader();
    if (event.getEvent() == XMLEvent.START_ELEMENT) {
      crsStr = reader.getAttributeValue(null, "srsName");
    }
  }
  
  /**
   * @return the first CRS found in the indexed chunk
   */
  public String getCRS() {
    return crsStr;
  }

  @Override
  public Map<String, Object> getResult() {
    if (crsStr == null) {
      // no CRS found
      return ImmutableMap.of();
    }
    return ImmutableMap.of("crs", crsStr);
  }
}
