package io.georocket.index;

import java.util.Map;

/**
 * Indexes chunks
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface Indexer {
  /**
   * Will be called when the whole chunk has been passed to the indexer
   * @return the results that should be added to the index or an empty
   * map if nothing should be added (never <code>null</code>)
   */
  Map<String, Object> getResult();
}
