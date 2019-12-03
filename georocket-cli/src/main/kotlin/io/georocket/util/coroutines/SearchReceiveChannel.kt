package io.georocket.util.coroutines

import io.georocket.client.SearchReadStream
import io.georocket.client.SearchReadStreamResult
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.channels.ReceiveChannel

class SearchReceiveChannel(
    delegate: ReceiveChannel<Buffer>,
    private val searchReadStream: SearchReadStream
) : ReceiveChannel<Buffer> by delegate {
  val result: SearchReadStreamResult get() = searchReadStream.result
}
