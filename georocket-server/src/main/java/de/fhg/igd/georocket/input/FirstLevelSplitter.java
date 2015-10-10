package de.fhg.igd.georocket.input;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

/**
 * Splits incoming XML tokens whenever a token in the first level (i.e. a
 * child of the XML document's root node) is encountered
 * @author Michel Kraemer
 */
public class FirstLevelSplitter extends XMLSplitter {
  private int depth = 0;
  
  /**
   * Create splitter
   * @param window a buffer for incoming data
   * @param xmlReader the XML reader that emits the event that
   * this splitter will handle
   */
  public FirstLevelSplitter(Window window, XMLStreamReader xmlReader) {
    super(window, xmlReader);
  }
  
  @Override
  protected String onXMLEvent(int event, int pos) {
    String result = null;
    
    // create new chunk if we're just after the end of a first-level element
    if (depth == 1 && isMarked()) {
      result = makeChunk(pos);
    }
    
    switch (event) {
    case XMLEvent.START_ELEMENT:
      if (depth == 1) {
        mark(pos);
      }
      ++depth;
      break;
    case XMLEvent.END_ELEMENT:
      --depth;
      break;
    default:
      break;
    }
    
    return result;
  }
}
