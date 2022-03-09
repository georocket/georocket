package io.georocket.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow

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

suspend fun <T> Flow<T>.chunked(chunkSize: Int): Flow<List<T>> {
  return flow {
    var chunk = mutableListOf<T>()
    collect { element ->
      chunk.add(element)
      if (chunk.size >= chunkSize) {
        emit(chunk)
        chunk = mutableListOf()
      }
    }
    if (chunk.isNotEmpty()) {
      emit(chunk)
    }
  }
}
