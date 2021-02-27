package io.georocket.output

import io.georocket.storage.ChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

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
  suspend fun merge(chunk: Buffer, chunkMetadata: T, outputStream: WriteStream<Buffer>)

  /**
   * Finishes merging chunks. Write remaining bytes into the given [outputStream]
   */
  fun finish(outputStream: WriteStream<Buffer>)
}
