package io.georocket.tasks

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.Instant

/**
 * A task currently being performed by GeoRocket
 * @author Michel Kraemer
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  JsonSubTypes.Type(value = ImportingTask::class, name = "importing"),
  JsonSubTypes.Type(value = ReceivingTask::class, name = "receiving")
)
@JsonInclude(value = JsonInclude.Include.NON_NULL)
interface Task {
  /**
   * The task's unique ID
   */
  val id: String

  /**
   * The correlation ID the task belongs to
   */
  val correlationId: String

  /**
   * The time when GeoRocket has started to execute the task
   */
  val startTime: Instant

  /**
   * The time when GeoRocket has finished executing the task (may be `null`
   * if GeoRocket has not finished the task yet)
   */
  val endTime: Instant?

  /**
   * The error that occurred during the execution of the task (`null` if no
   * error has occurred)
   */
  val error: TaskError?
}
