package io.georocket.util

import io.vertx.core.Vertx
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Tests for [debounce]
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
class DebounceTest {
  /**
   * Simple test case
   */
  @Test
  fun simple(vertx: Vertx, ctx: VertxTestContext) {
    GlobalScope.launch(vertx.dispatcher()) {
      var calls = 0
      val f = debounce(vertx, 100) {
        calls++
      }

      // call f again and again
      for (i in 0..100) {
        f()
      }

      // ... but check that only 1 call will actually be performed after 100 ms
      delay(150)
      ctx.verify {
        assertThat(calls).isEqualTo(1)
      }
      ctx.completeNow()
    }
  }
}
