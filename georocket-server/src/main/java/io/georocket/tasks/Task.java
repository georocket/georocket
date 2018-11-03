package io.georocket.tasks;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Calendar;

/**
 * A task currently being performed by GeoRocket
 * @author Michel Kraemer
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = IndexingTask.class, name = "indexing"),
  @JsonSubTypes.Type(value = ImportingTask.class, name = "importing"),
  @JsonSubTypes.Type(value = ReceivingTask.class, name = "receiving")
})
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public interface Task {
  /**
   * Get the correlation ID the task belongs to
   * @return the ID
   */
  String getCorrelationId();

  /**
   * Get the time when GeoRocket has started to execute the task
   * @return the task's start time (may be {@code null} if GeoRocket has not
   * yet started executing the task)
   */
  Calendar getStartTime();

  /**
   * Get the time when GeoRocket has finished executing the task. Note that
   * some tasks never finish because their end cannot be decided. In this case,
   * the method always returns {@code null}.
   * @return the task's end time (may be {@code null} if GeoRocket has not
   * finished the task yet or if the task's end cannot be decided)
   */
  Calendar getEndTime();

  /**
   * Increment the values from this task by the values from the given one
   * @param other the task to merge into this one
   * @throws IllegalArgumentException if the given task is not compatible to
   * this one
   */
  void inc(Task other);
}
