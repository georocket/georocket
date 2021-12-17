package io.georocket.util;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;

import org.junit.Test;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

/**
 * Test {@link ThrowableHelper}
 * @author Andrej Sajenko
 */
public class ThrowableHelperTest {
  /**
   * Test code of {@link ReplyException}
   */
  @Test
  public void testThrowableToCodeReplyException() {
    int expectedCode = 505;
    
    Throwable throwable = new ReplyException(ReplyFailure.NO_HANDLERS, expectedCode, "Message");
    
    int statusCode = ThrowableHelper.throwableToCode(throwable);
    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test code of {@link IllegalArgumentException}
   */
  @Test
  public void testThrowableToCodeIllegalArgument() {
    int expectedCode = 400;
    
    Throwable throwable = new IllegalArgumentException();
    
    int statusCode = ThrowableHelper.throwableToCode(throwable);
    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test code of {@link FileNotFoundException}
   */
  @Test
  public void testThrowableToCodeFileNotFound() {
    int expectedCode = 404;
    
    Throwable throwable = new FileNotFoundException();
    
    int statusCode = ThrowableHelper.throwableToCode(throwable);
    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test code of unknown throwables.
   */
  @Test
  public void testThrowableToCodeThrowable() {
    int expectedCode = 500;
    
    Throwable throwable = new Throwable();
    
    int statusCode = ThrowableHelper.throwableToCode(throwable);
    assertEquals(expectedCode, statusCode);
  }

  /**
   * Test throwable message
   */
  @Test
  public void testThrowableToMessage() {
    String expectedMessage = "A Error happen!";
    String defaultMessage = "Oops!";
    
    Throwable throwable = new Throwable(expectedMessage);
    
    String message = ThrowableHelper.throwableToMessage(throwable, defaultMessage);
    assertEquals(expectedMessage, message);
  }

  /**
   * Test default message
   */
  @Test
  public void testThrowableToMessageDefault() {
    String defaultMessage = "Oops!";
    
    Throwable throwable = new Throwable();
    
    String message = ThrowableHelper.throwableToMessage(throwable, defaultMessage);
    assertEquals(defaultMessage, message);
  }
}
