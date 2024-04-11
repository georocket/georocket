package io.georocket.util

import com.github.benmanes.caffeine.cache.AsyncCache
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import java.util.concurrent.CompletableFuture

suspend fun <K, V> AsyncCache<K, V>.getAwait(key: K, loader: suspend (key: K) -> V): V {
  return get(key) { _, executor ->
    val f = CompletableFuture<V>()
    CoroutineScope(executor.asCoroutineDispatcher()).launch {
      f.complete(loader(key))
    }
    f
  }.await()
}
