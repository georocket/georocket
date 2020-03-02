package io.georocket.util.coroutines

import io.georocket.client.SearchParams
import io.georocket.client.StoreClient
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.toChannel
import kotlinx.coroutines.channels.ReceiveChannel

suspend fun StoreClient.appendTags(query: String?, layer: String?, tags: List<String>?) {
  awaitResult<Void> { appendTags(query, layer, tags, it) }
}

suspend fun StoreClient.delete(query: String?, layer: String?) {
  awaitResult<Void> { delete(query, layer, it) }
}

suspend fun StoreClient.getPropertyValues(property: String?, query: String?,
    layer: String?): ReceiveChannel<Buffer> {
  return awaitResult { ar ->
    getPropertyValues(property, query, layer) { res ->
      if (res.failed()) {
        ar.handle(Future.failedFuture(res.cause()))
      } else {
        val rs = res.result()
        val channel = rs.toChannel(Vertx.currentContext().owner())
        ar.handle(Future.succeededFuture(channel))
      }
    }
  }
}

suspend fun StoreClient.removeProperties(query: String?, layer: String?,
    properties: List<String>?) {
  awaitResult<Void> { removeProperties(query, layer, properties, it) }
}

suspend fun StoreClient.removeTags(query: String?, layer: String?,
    tags: List<String>?) {
  awaitResult<Void> { removeTags(query, layer, tags, it) }
}

suspend fun StoreClient.search(params: SearchParams): SearchReceiveChannel {
  return awaitResult { ar ->
    search(params) { res ->
      if (res.failed()) {
        ar.handle(Future.failedFuture(res.cause()))
      } else {
        val srs = res.result().response
        val channel = srs.toChannel(Vertx.currentContext().owner())
        val src = SearchReceiveChannel(channel, srs)
        ar.handle(Future.succeededFuture(src))
      }
    }
  }
}

suspend fun StoreClient.setProperties(query: String?, layer: String?,
    properties: List<String>?) {
  awaitResult<Void> { setProperties(query, layer, properties, it) }
}
