package io.georocket.input;

import io.georocket.storage.ChunkMeta;
import io.georocket.util.StreamEvent;
import io.vertx.core.buffer.Buffer;

/**
 * Splits input tokens and returns chunks
 * @author Michel Kraemer
 * @param <E> the type of the stream events this splitter can process
 * @param <M> the type of the chunk metadata created by this splitter
 */
public interface Splitter<E extends StreamEvent, M extends ChunkMeta> {
  /**
   * Result of the {@link Splitter#onEvent(StreamEvent)} method. Holds
   * a chunk and its metadata.
   * @param <M> the type of the metadata
   */
  class Result<M extends ChunkMeta> {
    private final Buffer chunk;
    private final Buffer prefix;
    private final Buffer suffix;
    private final M meta;
    
    /**
     * Create a new result object
     * @param chunk the chunk
     * @param meta the chunk's metadata
     */
    public Result(Buffer chunk, Buffer prefix, Buffer suffix, M meta) {
      this.chunk = chunk;
      this.meta = meta;
      this.prefix = prefix;
      this.suffix = suffix;
    }
    
    /**
     * @return the chunk
     */
    public Buffer getChunk() {
      return chunk;
    }

    public Buffer getPrefix() {
      return prefix;
    }

    public Buffer getSuffix() {
      return suffix;
    }

    /**
     * @return the chunk's metadata
     */
    public M getMeta() {
      return meta;
    }
  }
  
  /**
   * Will be called on every stream event
   * @param event the stream event
   * @return a new {@link Result} object (containing chunk and metadata) or
   * <code>null</code> if no result was produced
   */
  Result<M> onEvent(E event);
}
