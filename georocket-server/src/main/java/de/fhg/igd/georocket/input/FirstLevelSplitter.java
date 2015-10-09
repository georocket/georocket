package de.fhg.igd.georocket.input;

import java.nio.charset.StandardCharsets;

import javax.xml.stream.events.XMLEvent;

/**
 * Splits incoming XML tokens whenever a token in the first level (i.e. a
 * child of the XML document's root node) is encountered
 * @author Michel Kraemer
 */
public class FirstLevelSplitter implements Splitter {
  private int depth = 0;
  private int lastStart = -1;
  private final Window window;
  
  /**
   * Create splitter
   * @param window a buffer for incoming data
   */
  public FirstLevelSplitter(Window window) {
    this.window = window;
  }
  
  @Override
  public String onEvent(int event, int pos) {
    String result = null;
    
    // create new chunk if we're just after the end of a first-level element
    if (depth == 1 && event != XMLEvent.END_ELEMENT && lastStart >= 0) {
      byte[] bytes = window.getBytes(lastStart, pos);
      result = new String(bytes, StandardCharsets.UTF_8);
      window.advanceTo(pos);
      lastStart = -1;
    }
    
    switch (event) {
    case XMLEvent.START_ELEMENT:
      if (depth == 1) {
        lastStart = pos;
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
