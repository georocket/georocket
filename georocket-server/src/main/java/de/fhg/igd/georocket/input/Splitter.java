package de.fhg.igd.georocket.input;

/**
 * Splits XML tokens and returns chunks
 * @author Michel Kraemer
 */
public interface Splitter {
  /**
   * Will be called on every XML token
   * @param event the XML event
   * @param pos the position of the XML event in the XML file
   * @return a new chunk or <code>null</code> if no chunk was produced
   */
  String onEvent(int event, int pos);
}
