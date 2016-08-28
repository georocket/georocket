package io.georocket.input.xml;

import javax.xml.stream.events.XMLEvent;

import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.Window;
import io.georocket.util.XMLStreamEvent;

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
   */
  public FirstLevelSplitter(Window window) {
    super(window);
  }
  
  @Override
  protected Result<XMLChunkMeta> onXMLEvent(XMLStreamEvent event) {
    Result<XMLChunkMeta> result = null;
    
    // create new chunk if we're just after the end of a first-level element
    if (depth == 1 && isMarked()) {
      result = makeResult(event.getPos());
    }
    
    switch (event.getEvent()) {
    case XMLEvent.START_ELEMENT:
      if (depth == 1) {
        mark(event.getPos());
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
