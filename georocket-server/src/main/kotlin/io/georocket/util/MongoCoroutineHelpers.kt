package io.georocket.util

import com.mongodb.reactivestreams.client.MongoCollection
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.suspendCancellableCoroutine
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * A subscriber that requests exactly one object and returns `null` if
 * no object was published
 * @author Michel Kraemer
 */
private class OneSubscriber<T>(private val cont: CancellableContinuation<T?>) : Subscriber<T> {
  private var resumed = false

  override fun onComplete() {
    if (!resumed) {
      cont.resume(null)
      resumed = true
    }
  }

  override fun onSubscribe(s: Subscription) {
    s.request(1)
  }

  override fun onNext(t: T?) {
    if (!resumed) {
      cont.resume(t)
      resumed = true
    }
  }

  override fun onError(t: Throwable) {
    if (!resumed) {
      cont.resumeWithException(t)
      resumed = true
    }
  }
}

/**
 * Converts a function [f] returning a [Publisher] to a coroutine
 */
private suspend fun <T> wrapCoroutine(f: () -> Publisher<T>): T? {
  return suspendCancellableCoroutine { cont: CancellableContinuation<T?> ->
    f().subscribe(OneSubscriber(cont))
  }
}

suspend fun <T> MongoCollection<T>.insertOneAwait(document: T) {
  wrapCoroutine {
    insertOne(document)
  }
}
