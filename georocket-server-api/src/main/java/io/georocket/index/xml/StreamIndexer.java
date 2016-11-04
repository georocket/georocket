package io.georocket.index.xml;

import io.georocket.index.Indexer;
import io.georocket.util.StreamEvent;

/**
 * Indexes chunks that can processed in a streaming manner
 * @since 1.0.0
 * @author Michel Kraemer
 * @param <T> the type of the stream events this indexer can handle
 */
public interface StreamIndexer<T extends StreamEvent> extends Indexer {
  /**
   * Will be called on every stream event in the chunk
   * @param event the event
   */
  void onEvent(T event);
}
