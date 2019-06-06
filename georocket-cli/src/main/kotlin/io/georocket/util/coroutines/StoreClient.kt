package io.georocket.util.coroutines

import io.georocket.client.SearchParams
import io.georocket.client.SearchResult
import io.georocket.client.StoreClient
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.ReadStream
import io.vertx.kotlin.coroutines.awaitResult

suspend fun StoreClient.appendTags(query: String?, layer: String?, tags: List<String>?) {
  awaitResult<Void> { appendTags(query, layer, tags, it) }
}

suspend fun StoreClient.delete(query: String?, layer: String?) {
  awaitResult<Void> { delete(query, layer, it) }
}

suspend fun StoreClient.getPropertyValues(property: String?, query: String?,
    layer: String?): ReadStream<Buffer> {
  return awaitResult { getPropertyValues(property, query, layer, it) }
}

suspend fun StoreClient.removeProperties(query: String?, layer: String?,
    properties: List<String>?) {
  awaitResult<Void> { removeProperties(query, layer, properties, it) }
}

suspend fun StoreClient.removeTags(query: String?, layer: String?,
    tags: List<String>?) {
  awaitResult<Void> { removeTags(query, layer, tags, it) }
}

suspend fun StoreClient.search(params: SearchParams): SearchResult {
  return awaitResult { search(params, it) }
}

suspend fun StoreClient.setProperties(query: String?, layer: String?,
    properties: List<String>?) {
  awaitResult<Void> { setProperties(query, layer, properties, it) }
}
