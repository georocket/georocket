package io.georocket.util;

/**
 * An exception that will be created if an HTTP error has happened
 * @author Michel Kraemer
 * @since 1.2.0
 */
public class HttpException extends Exception {
  private static final long serialVersionUID = -6784905885779199095L;
  
  private final int statusCode;
  private final String statusMessage;
  
  /**
   * Create a new HTTP exception
   * @param statusCode the HTTP status code
   */
  public HttpException(int statusCode) {
    this(statusCode, null);
  }
  
  /**
   * Create a new HTTP exception
   * @param statusCode the HTTP status code
   * @param statusMessage the status message (may be null)
   */
  public HttpException(int statusCode, String statusMessage) {
    super(statusMessage == null ? String.valueOf(statusCode) :
            statusCode + " " + statusMessage);
    this.statusCode = statusCode;
    this.statusMessage = statusMessage;
  }

  /**
   * Create a new HTTP exception with the specified status code,
   * status message, suppression enabled or disabled, and writable stack
   * trace enabled or disabled.
   * @param statusCode the HTTP status code
   * @param statusMessage the status message (may be null)
   * @param enableSuppression whether or not suppression is enabled
   * or disabled
   * @param writableStackTrace whether or not the stack trace should
   * be writable
   */
  public HttpException(int statusCode, String statusMessage,
      boolean enableSuppression, boolean writableStackTrace) {
    super(statusMessage == null ? String.valueOf(statusCode) :
                    statusCode + " " + statusMessage,
      null, enableSuppression, writableStackTrace);
    this.statusCode = statusCode;
    this.statusMessage = statusMessage;
  }
  
  /**
   * @return the HTTP status code
   */
  public int getStatusCode() {
    return statusCode;
  }
  
  /**
   * @return the status message
   */
  public String getStatusMessage() {
    return statusMessage;
  }
}
