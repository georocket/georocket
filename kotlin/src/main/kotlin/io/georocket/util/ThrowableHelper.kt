package io.georocket.util

import io.georocket.ServerAPIException
import io.vertx.core.eventbus.ReplyException
import org.antlr.v4.runtime.misc.ParseCancellationException
import java.io.FileNotFoundException

/**
 * Helper class for [Throwable]s
 * @author Michel Kraemer
 * @since 1.2.0
 */
object ThrowableHelper {
  /**
   * Convert a throwable to an HTTP status code
   * @param t the throwable to convert
   * @return the HTTP status code
   */
  fun throwableToCode(t: Throwable?): Int {
    return when (t) {
      is ReplyException -> t.failureCode()
      is IllegalArgumentException, is ParseCancellationException -> 400
      is FileNotFoundException -> 404
      is HttpException -> t.statusCode
      else -> 500
    }
  }

  /**
   * Get the given throwable's message or return a default one if it is
   * `null`
   * @param t the throwable's message
   * @param defaultMessage the message to return if the one of the throwable
   * is `null`
   * @return the message
   */
  fun throwableToMessage(t: Throwable, defaultMessage: String): String {
    return if (t is ServerAPIException) {
      t.toJson().toString()
    } else t.message ?: return defaultMessage
  }
}
