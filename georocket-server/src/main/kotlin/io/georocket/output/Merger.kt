package io.georocket.output

import io.georocket.storage.ChunkMeta
import io.georocket.storage.ChunkReadStream
import java.lang.Void
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import io.vertx.rx.java.RxHelper
import rx.Completable

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
  fun init(chunkMetadata: T): Completable

  /**
   * Merge a [chunk] using the current merge strategy and information from the
   * given [chunkMetadata] into the given [outputStream]. The chunk should have
   * been passed to [init] first. If it hasn't, the method may or may not
   * accept it. If the chunk cannot be merged with the current strategy,
   * the returned observable will fail.
   * @return a Completable that will complete when the chunk has been merged
   */
  fun merge(chunk: ChunkReadStream, chunkMetadata: T,
      outputStream: WriteStream<Buffer>): Completable

  /**
   * Finishes merging chunks. Write remaining bytes into the given [outputStream]
   */
  fun finish(outputStream: WriteStream<Buffer>)

  /**
   * Write a [chunk] unchanged to an [outputStream] without doing further checks
   */
  fun writeChunk(chunk: ChunkReadStream, chunkMetadata: ChunkMeta,
      outputStream: WriteStream<Buffer>): Completable {
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

    val o = RxHelper.observableFuture<Void>()
    val handler = o.toHandler()

    chunk.exceptionHandler { err ->
      chunk.endHandler(null)
      handler.handle(Future.failedFuture(err))
    }

    chunk.endHandler { handler.handle(Future.succeededFuture()) }

    return o.toCompletable()
  }
}
