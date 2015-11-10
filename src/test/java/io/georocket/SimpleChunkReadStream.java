package io.georocket;

import io.georocket.storage.ChunkReadStream;
import io.georocket.util.DelegateReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * A simple read stream for chunks. Wraps around another read stream that
 * doesn't need to be closed or is closed by the caller.
 * @author Michel Kraemer
 */
public class SimpleChunkReadStream extends DelegateReadStream<Buffer> implements ChunkReadStream {
  private final long size;
  
  /**
   * Constructs a new read stream
   * @param size the chunk's size
   * @param delegate the underlying read stream
   */
  public SimpleChunkReadStream(long size, ReadStream<Buffer> delegate) {
    super(delegate);
    this.size = size;
  }
  
  @Override
  public long getSize() {
    return size;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
