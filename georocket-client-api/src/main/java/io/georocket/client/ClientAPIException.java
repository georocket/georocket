package io.georocket.client;

import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;

/**
 * An exception that will be created if an API exception has happened
 * on the client-side
 * @author Tim Hellhake
 * @since 1.1.0
 */
public class ClientAPIException extends NoStackTraceThrowable {
  private static final long serialVersionUID = 6875097931379406123L;
  private String type;

  /**
   * Create a new exception
   * @param type the type of the error
   * @param reason the reason for the error
   */
  public ClientAPIException(String type, String reason) {
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
   * Parse an API exception from a JSON string
   * @param json the JSON string
   * @return the exception
   */
  public static ClientAPIException parse(String json) {
    try {
      JsonObject obj = new JsonObject(json).getJsonObject("error", new JsonObject());
      return new ClientAPIException(obj.getString("type"), obj.getString("reason"));
    } catch (Exception e) {
      return new ClientAPIException("malformed_error_response", json);
    }
  }
}
