package de.fhg.igd.georocket.storage.file;

import de.fhg.igd.georocket.util.DelegateReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * A read stream for chunks
 * @author Michel Kraemer
 */
public class ChunkReadStream extends DelegateReadStream<Buffer> {
  private final long size;
  
  /**
   * Constructs a new read stream
   * @param size the chunk's size
   * @param delegate the underlying read stream
   */
  public ChunkReadStream(long size, ReadStream<Buffer> delegate) {
    super(delegate);
    this.size = size;
  }
  
  /**
   * @return the chunk's size
   */
  public long getSize() {
    return size;
  }
}
