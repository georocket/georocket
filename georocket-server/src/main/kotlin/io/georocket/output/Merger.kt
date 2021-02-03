package io.georocket.output

import io.georocket.storage.ChunkMeta
import io.georocket.storage.ChunkReadStream
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import io.vertx.kotlin.coroutines.await
import java.lang.IllegalStateException

/**
 * Merges chunks to create a valid output document
 * @author Michel Kraemer
 * @param <T> the ChunkMeta type
 */
interface Merger<T : ChunkMeta> {
  /**
   * Initialize this merger with the given [chunkMetadata] and determine the
   * merge strategy. This method must be called for all chunks that should be
   * merged. After [merge] has been called this method must not be called anymore.
   */
  fun init(chunkMetadata: T)

  /**
   * Merge a [chunk] using the current merge strategy and information from the
   * given [chunkMetadata] into the given [outputStream]. The chunk should have
   * been passed to [init] first. If it hasn't, the method may or may not
   * accept it. If the chunk cannot be merged with the current strategy,
   * the method will throw and [IllegalStateException]
   */
  suspend fun merge(chunk: ChunkReadStream, chunkMetadata: T, outputStream: WriteStream<Buffer>)

  /**
   * Finishes merging chunks. Write remaining bytes into the given [outputStream]
   */
  fun finish(outputStream: WriteStream<Buffer>)

  /**
   * Write a [chunk] unchanged to an [outputStream] without doing further checks
   */
  suspend fun writeChunk(chunk: ChunkReadStream, chunkMetadata: ChunkMeta,
      outputStream: WriteStream<Buffer>) {
    // write chunk to output stream
    var start = chunkMetadata.start
    var end = chunkMetadata.end
    chunk.handler { buf: Buffer ->
      val s = start.coerceAtMost(buf.length()).coerceAtLeast(0)
      val e = end.coerceAtMost(buf.length()).coerceAtLeast(0)
      if (s != e) {
        outputStream.write(buf.getBuffer(s, e))
      }
      start -= buf.length()
      end -= buf.length()
    }

    val p = Promise.promise<Unit>()

    chunk.exceptionHandler { err ->
      chunk.endHandler(null)
      p.fail(err)
    }

    chunk.endHandler { p.complete() }

    return p.future().await()
  }
}
