package de.fhg.igd.georocket.storage.file;

import de.fhg.igd.georocket.storage.ChunkReadStream;
import de.fhg.igd.georocket.util.DelegateReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;

/**
 * A read stream for chunks
 * @author Michel Kraemer
 */
public class FileChunkReadStream extends DelegateReadStream<Buffer> implements ChunkReadStream {
  private final long size;
  private final AsyncFile file;
  
  /**
   * Constructs a new read stream
   * @param size the chunk's size
   * @param delegate the underlying read stream
   */
  public FileChunkReadStream(long size, AsyncFile delegate) {
    super(delegate);
    this.size = size;
    this.file = delegate;
  }
  
  /**
   * @return the chunk's size
   */
  public long getSize() {
    return size;
  }

  @Override
  public void close() {
    file.close();
  }
}
