package io.georocket.input

import io.georocket.storage.ChunkMeta
import io.georocket.util.StreamEvent
import io.vertx.core.buffer.Buffer

/**
 * Splits input tokens and returns chunks
 * @author Michel Kraemer
 * @param <E> the type of the stream events this splitter can process
 * @param <M> the type of the chunk metadata created by this splitter
 */
interface Splitter<E : StreamEvent, M : ChunkMeta> {
  /**
   * Result of the [Splitter.onEvent] method. Holds
   * a chunk and its metadata.
   * @param <M> the type of the metadata
   */
  data class Result<out M : ChunkMeta>(
    val chunk: Buffer,
    val prefix: Buffer?,
    val suffix: Buffer?,
    val meta: M
  )

  /**
   * Will be called on every stream event
   * @param event the stream event
   * @return a new [Result] object (containing chunk and metadata) or
   * `null` if no result was produced
   */
  fun onEvent(event: E): Result<M>?
}
