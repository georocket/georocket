package io.georocket.index

import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.Caffeine
import io.georocket.storage.ChunkMeta
import java.time.Duration

/**
 * Abstract base class for all [Index] implementations
 * @author Michel Kraemer
 */
abstract class AbstractIndex {
  companion object {
    /**
     * A cache used when importing chunk metadata, so that the same objects are
     * not inserted over and over again
     */
    @JvmStatic
    protected val addedChunkMetaCache: AsyncCache<ChunkMeta, String> = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(Duration.ofDays(1))
      .buildAsync()

    /**
     * A cache used when loading chunk metadata by ID
     */
    @JvmStatic
    protected val loadedChunkMetaCache: AsyncCache<String, ChunkMeta> = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(Duration.ofDays(1))
      .buildAsync()
  }

}
