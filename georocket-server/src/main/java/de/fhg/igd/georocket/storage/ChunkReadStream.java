package de.fhg.igd.georocket.storage;

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
   * Close the stream and release all resources
   */
  void close();
}
