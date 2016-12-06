package io.georocket.index.xml;

import io.georocket.index.Indexer;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.IndexMeta;

/**
 * Indexes chunks based on their metadata
 * @since 1.0.0
 * @author Michel Kraemer
 */
public interface MetaIndexer extends Indexer {
  /**
   * Will be called when a chunk is being indexed
   * @param path the chunk's path in the GeoRocket store
   * @param chunkMeta the chunk metadata
   * @param indexMeta the metadata used for indexing
   */
  void onIndexChunk(String path, ChunkMeta chunkMeta, IndexMeta indexMeta);
}
