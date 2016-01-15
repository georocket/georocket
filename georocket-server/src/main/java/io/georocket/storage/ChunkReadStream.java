package io.georocket.storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * A read stream for chunks
 * @author Michel Kraemer
 */
public interface ChunkReadStream extends ReadStream<Buffer> {
  /**
   * @return the chunk's size
   */
  long getSize();
  
  /**
   * Close the stream and release all resources. The actual operation happens
   * asynchronously.
   */
  default void close() {
    close(null);
  }
  
  /**
   * Close the stream and release all resources
   * @param handler will be called when the operation has finished (may be null)
   */
  void close(Handler<AsyncResult<Void>> handler);
}
