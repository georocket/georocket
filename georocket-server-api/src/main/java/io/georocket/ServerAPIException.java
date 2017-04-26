package io.georocket;

import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;

/**
 * An exception that will be created if an API exception has happened
 * on the server side
 * @author Tim Hellhake
 * @since 1.1.0
 */
public class ServerAPIException extends NoStackTraceThrowable {
  /**
   * The syntax of a property command is not valid
   */
  public static final String INVALID_PROPERTY_SYNTAX = "invalid_property_syntax";
  /**
   * The verticle replied with a simple error message, see reason for details
   */
  public static final String PROCESSING_ERROR = "processing_error";
  /**
   * The server issued a http request (e.g. store api or elasticsearch download)
   * which failed
   */
  public static final String HTTP_ERROR = "http_error";
  /**
   * A general error occurred during the client request, see reason for details
   */
  public static final String REQUEST_ERROR = "request_error";
  private static final long serialVersionUID = -4139618811295918617L;
  /**
   * A unique machine readable name for the api exception
   */
  private String type;

  /**
   * Create a new exception
   * @param type the type of the error
   * @param reason the reason for the error
   */
  public ServerAPIException(String type, String reason) {
    super(reason);
    this.type = type;
  }

  /**
   * Get the type of the error
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * Serialize exception
   * @return a JSON object with the type and the reason of this error
   */
  public JsonObject toJson() {
    return toJson(type, getMessage());
  }

  /**
   * Create a JSON object from a type and a reason
   * @param type the type
   * @param reason the reason
   * @return a JSON object with the specified type and the specified reason
   */
  public static JsonObject toJson(String type, String reason) {
    return new JsonObject().put("error",
      new JsonObject()
        .put("type", type)
        .put("reason", reason)
    );
  }
}
