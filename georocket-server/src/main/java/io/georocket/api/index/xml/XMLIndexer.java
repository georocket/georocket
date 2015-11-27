package io.georocket.api.index.xml;

import io.georocket.api.index.Indexer;
import io.georocket.util.XMLStreamEvent;

/**
 * Indexes XML chunks
 * @author Michel Kraemer
 */
public interface XMLIndexer extends Indexer {
  /**
   * Will be called on every XML event in the chunk
   * @param event the event
   */
  void onEvent(XMLStreamEvent event);
}
