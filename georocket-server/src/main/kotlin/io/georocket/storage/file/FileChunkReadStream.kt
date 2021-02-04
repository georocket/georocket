package io.georocket.storage.file

import io.georocket.util.io.DelegateReadStream
import io.georocket.storage.ChunkReadStream
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.AsyncFile

/**
 * A read stream for chunks
 * @author Michel Kraemer
 */
class FileChunkReadStream(private val size: Long, private val file: AsyncFile) :
    DelegateReadStream<Buffer>(file), ChunkReadStream {
  /**
   * @return the chunk's size
   */
  override fun getSize(): Long {
    return size
  }

  override fun close(handler: Handler<AsyncResult<Void>>) {
    file.close(handler)
  }
}
