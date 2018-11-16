package io.georocket.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.georocket.util.ThrowableHelper;

/**
 * An error that occurred during the execution of a task
 * @author Michel Kraemer
 */
public class TaskError {
  private final String type;
  private final String reason;

  /**
   * Construct a new error from a throwable
   * @param t the throwable
   */
  public TaskError(Throwable t) {
    this.type = t.getClass().getSimpleName();
    this.reason = ThrowableHelper.throwableToMessage(t, "Unknown reason");
  }

  /**
   * Construct a new error
   * @param type the error type
   * @param reason a human-readable string about the cause of the error
   */
  @JsonCreator
  public TaskError(@JsonProperty("type") String type, @JsonProperty("reason") String reason) {
    this.type = type;
    this.reason = reason;
  }

  /**
   * Get the error type
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * Get a human-readable string giving details about the cause of the error
   * @return the reason for the error
   */
  public String getReason() {
    return reason;
  }
}
