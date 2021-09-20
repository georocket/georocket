package io.georocket.index

import io.georocket.util.StreamEvent
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.channels.Channel

/**
 * Parses chunks and transforms them to stream events
 * @author Michel Kraemer
 */
interface Transformer<E : StreamEvent> {
  /**
   * Parse the given [chunk] and send the created stream events to the given
   * [destination] channel
   */
  suspend fun transformTo(chunk: Buffer, destination: Channel<E>)
}
