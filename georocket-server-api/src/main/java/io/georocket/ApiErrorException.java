package io.georocket;

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
   * Serialize exception
   *
   * @return a json object with the type and the reason of this error
   */
  public JsonObject toJson() {
    return toJson(type, getMessage());
  }

  /**
   * Create a json object from a type and a reason
   *
   * @param type   the type
   * @param reason the reason
   * @return a json object with the specified type and the specified reason
   */
  public static JsonObject toJson(String type, String reason) {
    return new JsonObject().put("error",
      new JsonObject()
        .put("type", type)
        .put("reason", reason)
    );
  }
}
