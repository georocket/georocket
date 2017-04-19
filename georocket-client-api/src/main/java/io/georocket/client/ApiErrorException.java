package io.georocket.client;

import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;

/**
 * An exception that will be created if an api error has happened
 *
 * @author Tim Hellhake
 */
public class ApiErrorException extends NoStackTraceThrowable {
  private String type;

  /**
   * Create a new exception
   *
   * @param type   the type of the error
   * @param reason the reason of the error
   */
  public ApiErrorException(String type, String reason) {
    super(reason);
    this.type = type;
  }

  /**
   * Get the type of the error
   *
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * Parse an api exception from a json string
   *
   * @param json the json string
   * @return the exception
   */
  public static ApiErrorException parse(String json) {
    try {
      JsonObject obj = new JsonObject(json).getJsonObject("error", new JsonObject());

      return new ApiErrorException(obj.getString("type"), obj.getString("reason"));
    } catch (Exception e) {
      return new ApiErrorException("malformed_error_response", json);
    }
  }
}
