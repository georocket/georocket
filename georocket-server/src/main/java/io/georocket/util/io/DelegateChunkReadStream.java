package io.georocket.util.io;

import io.georocket.BufferReadStream;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * A simple read stream for chunks. Wraps around another read stream that
 * doesn't need to be closed or is closed by the caller.
 * @author Michel Kraemer
 */
public class DelegateChunkReadStream extends DelegateReadStream<Buffer> implements ChunkReadStream {
  private final long size;
  
  /**
   * Constructs a new read stream
   * @param size the chunk's size
   * @param delegate the underlying read stream
   */
  public DelegateChunkReadStream(long size, ReadStream<Buffer> delegate) {
    super(delegate);
    this.size = size;
  }
  
  /**
   * Create a new read stream from a chunk
   * @param chunk the chunk
   */
  public DelegateChunkReadStream(Buffer chunk) {
    this(chunk.length(), new BufferReadStream(chunk));
  }
  
  @Override
  public long getSize() {
    return size;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    if (handler != null) {
      handler.handle(Future.succeededFuture());
    }
  }
}
