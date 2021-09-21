package io.georocket.util

import com.mongodb.client.result.DeleteResult
import com.mongodb.reactivestreams.client.MongoCollection
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.impl.JsonObjectBsonAdapter
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
 * A subscriber that requests as many objects as possible and collects them
 * in a list
 * @author Michel Kraemer
 */
private class CollectionSubscriber<T>(private val cont: CancellableContinuation<List<T>>) : Subscriber<T> {
  private val result = mutableListOf<T>()

  override fun onComplete() {
    cont.resume(result)
  }

  override fun onSubscribe(s: Subscription) {
    s.request(Long.MAX_VALUE)
  }

  override fun onNext(t: T) {
    result.add(t)
  }

  override fun onError(t: Throwable) {
    cont.resumeWithException(t)
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

suspend fun <T> MongoCollection<T>.findAwait(filter: JsonObject, limit: Int = -1,
  skip: Int = 0, sort: JsonObject? = null, projection: JsonObject? = null): List<T> {
  return suspendCancellableCoroutine { cont: CancellableContinuation<List<T>> ->
    var f = find(JsonObjectBsonAdapter(filter))
    if (limit >= 0) {
      f = f.limit(limit)
    }
    if (skip > 0) {
      f = f.skip(skip)
    }
    if (sort != null) {
      f = f.sort(JsonObjectBsonAdapter(sort))
    }
    if (projection != null) {
      f = f.projection(JsonObjectBsonAdapter(projection))
    }
    f.subscribe(CollectionSubscriber(cont))
  }
}

suspend fun <T> MongoCollection<T>.insertOneAwait(document: T) {
  wrapCoroutine {
    insertOne(document)
  }
}

suspend fun <T> MongoCollection<T>.insertManyAwait(documents: List<T>) {
  wrapCoroutine {
    insertMany(documents)
  }
}

suspend fun <T> MongoCollection<T>.deleteManyAwait(filter: JsonObject): DeleteResult {
  return wrapCoroutine {
    deleteMany(JsonObjectBsonAdapter(filter))
  } ?: throw IllegalStateException("Delete operation did not produce a result")
}
