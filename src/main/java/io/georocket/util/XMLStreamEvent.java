package io.georocket.util;

import javax.xml.stream.XMLStreamReader;

/**
 * An event produced by {@link AsyncXMLParser}
 * @author Michel Kraemer
 */
public class XMLStreamEvent {
  private final int event;
  private final int pos;
  private final XMLStreamReader xmlReader;
  
  /**
   * Constructs a new event
   * @param event the actual XML event type
   * @param pos the position in the XML stream where the event has occurred
   * @param xmlReader the XML reader that produced the event
   */
  public XMLStreamEvent(int event, int pos, XMLStreamReader xmlReader) {
    this.event = event;
    this.pos = pos;
    this.xmlReader = xmlReader;
  }
  
  /**
   * @return the actual XML event type
   * @see javax.xml.stream.XMLStreamConstants
   */
  public int getEvent() {
    return event;
  }
  
  /**
   * @return the position in the XML stream where the event has occurred
   */
  public int getPos() {
    return pos;
  }
  
  /**
   * @return the XML reader that produced the event
   */
  public XMLStreamReader getXMLReader() {
    return xmlReader;
  }
}
