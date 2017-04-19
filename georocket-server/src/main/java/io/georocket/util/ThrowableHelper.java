package io.georocket.util;

import java.io.FileNotFoundException;

import javax.xml.ws.http.HTTPException;

import io.georocket.ApiErrorException;
import io.vertx.core.eventbus.ReplyException;

/**
 * Helper class for {@link Throwable}s
 * @author Michel Kraemer
 */
public final class ThrowableHelper {
  private ThrowableHelper() {
    // hidden constructor
  }
  
  /**
   * Convert a throwable to an HTTP status code
   * @param t the throwable to convert
   * @return the HTTP status code
   */
  public static int throwableToCode(Throwable t) {
    if (t instanceof ReplyException) {
      return ((ReplyException)t).failureCode();
    } else if (t instanceof IllegalArgumentException) {
      return 400;
    } else if (t instanceof FileNotFoundException) {
      return 404;
    } else if (t instanceof HttpException) {
      return ((HttpException)t).getStatusCode();
    } else if (t instanceof HTTPException) {
      return ((HTTPException)t).getStatusCode();
    }
    return 500;
  }
  
  /**
   * Get the given throwable's message or return a default one if it is
   * <code>null</code>
   * @param t the throwable's message
   * @param defaultMessage the message to return if the one of the throwable
   * is <code>null</code>
   * @return the message
   */
  public static String throwableToMessage(Throwable t, String defaultMessage) {
    if (t instanceof ApiErrorException) {
      return ((ApiErrorException)t).toJson().toString();
    }

    String m = t.getMessage();
    if (m == null) {
      return defaultMessage;
    }
    return m;
  }
}
