package io.georocket.util;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.junit.Assert.*;

/**
 * Test {@link ThrowableHelper}
 * @author Andrej Sajenko
 */
public class ThrowableHelperTest {

  /**
   * Test code of {@link ReplyException}
   *
   * @throws Exception
   */
  @Test
  public void testThrowableToCode_ReplyException() throws Exception {
    int expectedCode = 505;

    Throwable throwable = new ReplyException(ReplyFailure.NO_HANDLERS, expectedCode, "Message");

    int statusCode = ThrowableHelper.throwableToCode(throwable);

    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test code of {@link IllegalArgumentException}
   *
   * @throws Exception
   */
  @Test
  public void testThrowableToCode_IllegalArgument() throws Exception {
    int expectedCode = 400;

    Throwable throwable = new IllegalArgumentException();

    int statusCode = ThrowableHelper.throwableToCode(throwable);

    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test code of {@link FileNotFoundException}
   *
   * @throws Exception
   */
  @Test
  public void testThrowableToCode_FileNotFound() throws Exception {
    int expectedCode = 404;

    Throwable throwable = new FileNotFoundException();

    int statusCode = ThrowableHelper.throwableToCode(throwable);

    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test code of unknown throwables.
   *
   * @throws Exception
   */
  @Test
  public void testThrowableToCode_Throwable() throws Exception {
    int expectedCode = 500;

    Throwable throwable = new Throwable();

    int statusCode = ThrowableHelper.throwableToCode(throwable);

    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test throwable message
   *
   * @throws Exception
   */
  @Test
  public void testThrowableToMessage_Message() throws Exception {
    String expectedMessage = "A Error happen!";
    String defaultMessage = "Oops!";

    Throwable throwable = new Throwable(expectedMessage);

    String message = ThrowableHelper.throwableToMessage(throwable, defaultMessage);

    assertEquals(expectedMessage, message);
  }

  /**
   * Test default message
   *
   * @throws Exception
   */
  @Test
  public void testThrowableToMessage_Default() throws Exception {
    String defaultMessage = "Oops!";

    Throwable throwable = new Throwable();

    String message = ThrowableHelper.throwableToMessage(throwable, defaultMessage);

    assertEquals(defaultMessage, message);
  }

}