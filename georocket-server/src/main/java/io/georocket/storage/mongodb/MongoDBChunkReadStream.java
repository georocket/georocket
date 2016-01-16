package io.georocket.storage.mongodb;

import java.io.InputStream;

import io.georocket.storage.ChunkReadStream;
import io.georocket.util.io.InputStreamReadStream;
import io.vertx.core.Vertx;

/**
 * A read stream for chunks stored in MongoDB
 * @author Michel Kraemer
 */
public class MongoDBChunkReadStream extends InputStreamReadStream implements ChunkReadStream {
  private final long size;
  
  /**
   * Constructs a new read stream
   * @param is the input stream containing the chunk
   * @param size the chunk size
   * @param vertx the Vert.x instance
   */
  public MongoDBChunkReadStream(InputStream is, long size, Vertx vertx) {
    super(is, vertx);
    this.size = size;
  }
  
  @Override
  public long getSize() {
    return size;
  }
}
