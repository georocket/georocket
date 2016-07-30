package io.georocket.storage.hdfs;

import java.io.InputStream;

import io.georocket.storage.ChunkReadStream;
import io.georocket.util.io.InputStreamReadStream;
import io.vertx.core.Vertx;

/**
 * A chunk read stream delegating to an input stream
 * @author Michel Kraemer
 */
public class InputStreamChunkReadStream extends InputStreamReadStream
    implements ChunkReadStream {
  private final long size;
  
  /**
   * Constructs a new read stream
   * @param is the input stream containing the chunk
   * @param size the chunk size
   * @param vertx the Vert.x instance
   */
  public InputStreamChunkReadStream(InputStream is, long size, Vertx vertx) {
    super(is, vertx);
    this.size = size;
  }
  
  @Override
  public long getSize() {
    return size;
  }
}
