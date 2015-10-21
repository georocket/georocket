package de.fhg.igd.georocket.index;

import java.util.Map;

import de.fhg.igd.georocket.util.XMLStreamEvent;

/**
 * Indexes chunks
 * @author Michel Kraemer
 */
public interface Indexer {
  /**
   * Will be called on every XML event in the chunk
   * @param event the event
   */
  void onEvent(XMLStreamEvent event);
  
  /**
   * Will be called when the whole chunk has been passed to the indexer
   * @return the results that should be added to the index or an empty
   * map if nothing should be added (never <code>null</code>)
   */
  Map<String, Object> getResult();
}
