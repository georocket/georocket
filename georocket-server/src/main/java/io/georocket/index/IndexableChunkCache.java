package io.georocket.index;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.georocket.constants.ConfigConstants;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A cache for chunks that are about to be indexed. The cache keeps chunks only
 * until they have been requested or until a configurable time has passed (see
 * {@link ConfigConstants#INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS}). The
 * cache has a configurable maximum size (see
 * {@link ConfigConstants#INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE}).
 * @author Michel Kraemer
 */
public class IndexableChunkCache {
  /**
   * A private class holding the singleton instance of this class
   */
  private static class LazyHolder {
    static final IndexableChunkCache INSTANCE = new IndexableChunkCache();
  }

  private final long maximumSize;
  private final Cache<String, Buffer> cache;
  private final AtomicLong size = new AtomicLong();

  /**
   * Get the current Vert.x context
   * @return the context
   * @throws RuntimeException if the method was not called from within a
   * Vert.x context
   */
  private static Context getCurrentContext() {
    Context ctx = Vertx.currentContext();
    if (ctx == null) {
      throw new RuntimeException("This class must be initiated within " +
        "a Vert.x context");
    }
    return ctx;
  }

  /***
   * Create a new cache
   */
  IndexableChunkCache() {
    this(getCurrentContext().config().getLong(ConfigConstants.INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE,
            ConfigConstants.DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE),
        getCurrentContext().config().getLong(ConfigConstants.INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS,
            ConfigConstants.DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS));
  }

  /**
   * Create a new cache
   * @param maximumSize the cache's maximum size in bytes
   * @param maximumTime the maximum number of seconds a chunk stays in the cache
   */
  IndexableChunkCache(long maximumSize, long maximumTime) {
    this.maximumSize = maximumSize;
    cache = CacheBuilder.newBuilder()
      .expireAfterWrite(maximumTime, TimeUnit.SECONDS)
      .<String, Buffer>removalListener(n -> size.addAndGet(-n.getValue().length()))
      .build();
  }

  /**
   * Gets the singleton instance of this class. Must be called from within a
   * Vert.x context.
   * @return the singleton instance
   * @throws RuntimeException if the method was not called from within a
   * Vert.x context
   */
  public static IndexableChunkCache getInstance() {
    return LazyHolder.INSTANCE;
  }

  /**
   * Adds a chunk to the cache. Do nothing if adding the chunk would exceed
   * the cache's maximum size
   * @param path the chunk's path
   * @param chunk the chunk
   */
  public void put(String path, Buffer chunk) {
    long chunkSize = chunk.length();
    long oldSize;
    long newSize;
    boolean cleanUpCalled = false;
    while (true) {
      oldSize = size.get();
      newSize = oldSize + chunkSize;
      if (newSize > maximumSize) {
        // make sure the chunk can be added if there were still some
        // removal events pending
        if (!cleanUpCalled) {
          cleanUpCalled = true;
          cache.cleanUp();
          continue;
        }
        return;
      }
      if (size.compareAndSet(oldSize, newSize)) {
        break;
      }
    }
    cache.put(path, chunk);
  }

  /**
   * Gets and removes a chunk from the cache
   * @param path the chunk's path
   * @return the chunk or {@code null} if the chunk was not found in the cache
   */
  public Buffer get(String path) {
    Buffer r = cache.getIfPresent(path);
    if (r != null) {
      cache.invalidate(path);
    }
    return r;
  }

  /**
   * Get the cache's size in bytes
   * @return the size
   */
  public long getSize() {
    return size.get();
  }

  /**
   * Get the number of chunks currently in the cache
   * @return the number of chunks
   */
  public long getNumberOfChunks() {
    return cache.size();
  }
}
