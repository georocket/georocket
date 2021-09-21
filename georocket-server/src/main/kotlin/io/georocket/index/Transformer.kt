package io.georocket.index

import io.georocket.util.StreamEvent
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

/**
 * Parses chunks and transforms them to stream events
 * @author Michel Kraemer
 */
interface Transformer<E : StreamEvent> {
  /**
   * Parse the given [buffer] to a flow of stream events. This method is a
   * combination of [transformChunk] and [finish]. It automatically closes the
   * parser and returns the remaining stream events.
   */
  suspend fun transform(buffer: Buffer) = flow {
    emitAll(transformChunk(buffer))
    emitAll(finish())
  }

  /**
   * Parse the given [chunk] to a flow of stream events. This method does not
   * automatically close the parser and can therefore be called multiple times.
   * After all chunks have been transformed, [finish] must be called to close
   * the parser and return the remaining stream events.
   */
  suspend fun transformChunk(chunk: Buffer): Flow<E>

  /**
   * Close the parser and return the remaining stream events. Only necessary
   * if [transformChunk] has been used.
   */
  suspend fun finish(): Flow<E>
}
