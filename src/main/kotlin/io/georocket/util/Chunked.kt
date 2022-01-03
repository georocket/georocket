package io.georocket.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector

suspend fun <T> Flow<T>.collectChunked(chunkSize: Int, collector: FlowCollector<Collection<T>>) {
  val chunk = mutableListOf<T>()
  this.collect { e ->
    chunk.add(e)
    if (chunk.size == chunkSize) {
      collector.emit(chunk)
      chunk.clear()
    }
  }
  if (chunk.isNotEmpty()) {
    collector.emit(chunk)
  }
}
