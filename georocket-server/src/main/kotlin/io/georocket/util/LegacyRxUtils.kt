package io.georocket.util

import kotlinx.coroutines.suspendCancellableCoroutine
import rx.Completable
import rx.CompletableSubscriber
import rx.Single
import rx.SingleSubscriber
import rx.Subscription
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

suspend fun <T> Single<T>.rxAwait(): T = suspendCancellableCoroutine { cont ->
  subscribe(object : SingleSubscriber<T>() {
    override fun onError(e: Throwable) {
      cont.resumeWithException(e)
    }

    override fun onSuccess(t: T) {
      cont.resume(t)
    }
  })
}

suspend fun Completable.rxAwait(): Unit = suspendCancellableCoroutine { cont ->
  subscribe(object : CompletableSubscriber {
    override fun onCompleted() {
      cont.resume(Unit)
    }

    override fun onError(e: Throwable) {
      cont.resumeWithException(e)
    }

    override fun onSubscribe(d: Subscription) {
    }
  })
}

