package io.georocket.util

import io.vertx.core.Vertx
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

/**
 * Wrap around a [block] and return a new function f. If f is called, it calls
 * [block] after [delay] milliseconds unless f is called again, in which case
 * [block] will be delayed further. Subsequent calls to f keep delaying
 * [block] until [delay] milliseconds have elapsed since the last call.
 */
fun CoroutineScope.debounce(vertx: Vertx, delay: Long = 100L, block: suspend () -> Unit): () -> Unit {
  var timer: Long? = null

  fun doIt() {
    timer?.let { vertx.cancelTimer(it) }
    timer = vertx.setTimer(delay) {
      timer = null
      launch {
        block()
      }
    }
  }

  return ::doIt
}
