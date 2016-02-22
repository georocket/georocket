package io.georocket.index.xml;

import io.georocket.index.Indexer;
import io.georocket.util.XMLStreamEvent;

/**
 * Indexes XML chunks
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface XMLIndexer extends Indexer {
  /**
   * Will be called on every XML event in the chunk
   * @param event the event
   */
  void onEvent(XMLStreamEvent event);
}
