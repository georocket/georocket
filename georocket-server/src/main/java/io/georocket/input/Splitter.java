package io.georocket.input;

import io.georocket.storage.ChunkMeta;
import io.georocket.util.StreamEvent;
import rx.Observable;

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
  public static class Result<M extends ChunkMeta> {
    private final String chunk;
    private final M meta;
    
    /**
     * Create a new result object
     * @param chunk the chunk
     * @param meta the chunk's metadata
     */
    public Result(String chunk, M meta) {
      this.chunk = chunk;
      this.meta = meta;
    }
    
    /**
     * @return the chunk
     */
    public String getChunk() {
      return chunk;
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
  
  /**
   * Observable version of {@link #onEvent(StreamEvent)}
   * @param event the stream event
   * @return an observable that will emit a {@link Result} object (containing
   * a chunk and metadata) or emit nothing if no chunk was produced
   */
  default Observable<Result<M>> onEventObservable(E event) {
    Result<M> result = onEvent(event);
    if (result == null) {
      return Observable.empty();
    }
    return Observable.just(result);
  }
}
