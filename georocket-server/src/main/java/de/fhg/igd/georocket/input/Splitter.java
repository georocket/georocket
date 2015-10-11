package de.fhg.igd.georocket.input;

import de.fhg.igd.georocket.util.XMLStreamEvent;

/**
 * Splits XML tokens and returns chunks
 * @author Michel Kraemer
 */
public interface Splitter {
  /**
   * Will be called on every XML event
   * @param event the XML event
   * @return a new chunk or <code>null</code> if no chunk was produced
   */
  String onEvent(XMLStreamEvent event);
}
