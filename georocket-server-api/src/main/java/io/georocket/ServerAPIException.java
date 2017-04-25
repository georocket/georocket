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
  private static final long serialVersionUID = -4139618811295918617L;
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
