package io.georocket.util.coroutines

import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.awaitResult

/**
 * Like [io.vertx.kotlin.coroutines.awaitBlocking] but multiple calls to this
 * method in the same Vert.x context will allow executions to run in parallel.
 */
suspend fun <T> awaitBlockingConcurrent(block: () -> T): T {
  return awaitResult { handler ->
    val ctx = Vertx.currentContext()
    ctx.executeBlocking<T>({ fut ->
      fut.complete(block())
    }, false, { ar ->
      handler.handle(ar)
    })
  }
}
