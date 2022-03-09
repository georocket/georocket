package io.georocket.util

import io.georocket.util.ThrowableHelper.throwableToCode
import io.georocket.util.ThrowableHelper.throwableToMessage
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.eventbus.ReplyFailure
import org.junit.Assert
import org.junit.Test
import java.io.FileNotFoundException

/**
 * Test [ThrowableHelper]
 * @author Andrej Sajenko
 */
class ThrowableHelperTest {
  /**
   * Test code of [ReplyException]
   */
  @Test
  fun testThrowableToCodeReplyException() {
    val expectedCode = 505
    val throwable: Throwable = ReplyException(ReplyFailure.NO_HANDLERS, expectedCode, "Message")
    val statusCode = throwableToCode(throwable)
    Assert.assertEquals(expectedCode.toLong(), statusCode.toLong())
  }

  /**
   * Test code of [IllegalArgumentException]
   */
  @Test
  fun testThrowableToCodeIllegalArgument() {
    val expectedCode = 400
    val throwable: Throwable = IllegalArgumentException()
    val statusCode = throwableToCode(throwable)
    Assert.assertEquals(expectedCode.toLong(), statusCode.toLong())
  }

  /**
   * Test code of [FileNotFoundException]
   */
  @Test
  fun testThrowableToCodeFileNotFound() {
    val expectedCode = 404
    val throwable: Throwable = FileNotFoundException()
    val statusCode = throwableToCode(throwable)
    Assert.assertEquals(expectedCode.toLong(), statusCode.toLong())
  }

  /**
   * Test code of unknown throwables.
   */
  @Test
  fun testThrowableToCodeThrowable() {
    val expectedCode = 500
    val throwable = Throwable()
    val statusCode = throwableToCode(throwable)
    Assert.assertEquals(expectedCode.toLong(), statusCode.toLong())
  }

  /**
   * Test throwable message
   */
  @Test
  fun testThrowableToMessage() {
    val expectedMessage = "A Error happen!"
    val defaultMessage = "Oops!"
    val throwable = Throwable(expectedMessage)
    val message = throwableToMessage(throwable, defaultMessage)
    Assert.assertEquals(expectedMessage, message)
  }

  /**
   * Test default message
   */
  @Test
  fun testThrowableToMessageDefault() {
    val defaultMessage = "Oops!"
    val throwable = Throwable()
    val message = throwableToMessage(throwable, defaultMessage)
    Assert.assertEquals(defaultMessage, message)
  }
}
