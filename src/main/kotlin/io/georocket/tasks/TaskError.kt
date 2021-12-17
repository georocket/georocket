package io.georocket.tasks

import io.georocket.util.ThrowableHelper

/**
 * An error that occurred during the execution of a task
 * @author Michel Kraemer
 */
data class TaskError(
  /**
   * The error type
   */
  val type: String,

  /**
   * A human-readable string giving details about the cause of the error
   */
  val reason: String,
) {
  /**
   * Construct a new error from a throwable
   * @param t the throwable
   */
  constructor(t: Throwable) : this(t.javaClass.simpleName,
    ThrowableHelper.throwableToMessage(t, "Unknown reason"))
}
