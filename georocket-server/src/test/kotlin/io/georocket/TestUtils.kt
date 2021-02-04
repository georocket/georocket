package io.georocket

import io.vertx.junit5.VertxTestContext
import org.assertj.core.api.AbstractThrowableAssert
import org.assertj.core.api.Assertions

/**
 * Similar to [VertxTestContext.verify] but supports coroutines
 */
suspend fun VertxTestContext.coVerify(block: suspend () -> Unit): VertxTestContext {
  try {
    block()
  } catch (t: Throwable) {
    failNow(t)
  }
  return this
}

/**
 * Similar to [Assertions.assertThatThrownBy] but supports coroutines
 */
suspend fun assertThatThrownBy(block: suspend () -> Unit): AbstractThrowableAssert<*, out Throwable> {
  val caught = try {
    block()
    null
  } catch (t: Throwable) {
    t
  }
  return Assertions.assertThatThrownBy { if (caught != null) throw caught }
}
