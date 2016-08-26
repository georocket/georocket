package io.georocket.input;

import io.georocket.storage.ChunkMeta;
import io.georocket.util.StreamEvent;
import rx.Observable;

/**
 * Splits input tokens and returns chunks
 * @author Michel Kraemer
 * @param <T> the type of the stream events this splitter can process
 */
public interface Splitter<T extends StreamEvent> {
  /**
   * Result of the {@link Splitter#onEvent(StreamEvent)} method. Holds
   * a chunk and its metadata.
   */
  public static class Result {
    private final String chunk;
    private final ChunkMeta meta;
    
    /**
     * Create a new result object
     * @param chunk the chunk
     * @param meta the chunk's metadata
     */
    public Result(String chunk, ChunkMeta meta) {
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
    public ChunkMeta getMeta() {
      return meta;
    }
  }
  
  /**
   * Will be called on every stream event
   * @param event the stream event
   * @return a new {@link Result} object (containing chunk and metadata) or
   * <code>null</code> if no result was produced
   */
  Result onEvent(T event);
  
  /**
   * Observable version of {@link #onEvent(StreamEvent)}
   * @param event the stream event
   * @return an observable that will emit a {@link Result} object (containing
   * a chunk and metadata) or emit nothing if no chunk was produced
   */
  default Observable<Result> onEventObservable(T event) {
    Result result = onEvent(event);
    if (result == null) {
      return Observable.empty();
    }
    return Observable.just(result);
  }
}
