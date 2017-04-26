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
   * @since 1.1.0
   */
  public static final String INVALID_PROPERTY_SYNTAX_ERROR = "invalid_property_syntax_error";

  /**
   * The server issued an HTTP request (e.g. store API or Elasticsearch query)
   * which failed
   * @since 1.1.0
   */
  public static final String HTTP_ERROR = "http_error";

  /**
   * A generic error occurred, see reason for details
   * @since 1.1.0
   */
  public static final String GENERIC_ERROR = "generic_error";

  private static final long serialVersionUID = -4139618811295918617L;

  /**
   * A unique machine readable type for the API exception
   */
  private String type;

  /**
   * Create a new exception
   * @param type the exception type
   * @param reason the reason for the exception
   */
  public ServerAPIException(String type, String reason) {
    super(reason);
    this.type = type;
  }

  /**
   * Get the exception type
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
