package io.georocket.util;

import javax.xml.stream.XMLStreamReader;

/**
 * An event produced during XML parsing
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class XMLStreamEvent extends StreamEvent {
  private final int event;
  private final XMLStreamReader xmlReader;
  
  /**
   * Constructs a new event
   * @param event the actual XML event type
   * @param pos the position in the XML stream where the event has occurred
   * @param xmlReader the XML reader that produced the event
   */
  public XMLStreamEvent(int event, int pos, XMLStreamReader xmlReader) {
    super(pos);
    this.event = event;
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
   * @return the XML reader that produced the event
   */
  public XMLStreamReader getXMLReader() {
    return xmlReader;
  }
}
