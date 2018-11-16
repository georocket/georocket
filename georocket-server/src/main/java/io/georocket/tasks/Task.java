package io.georocket.tasks;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.georocket.util.InstantDeserializer;

import java.time.Instant;
import java.util.List;

/**
 * A task currently being performed by GeoRocket
 * @author Michel Kraemer
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ImportingTask.class, name = "importing"),
  @JsonSubTypes.Type(value = IndexingTask.class, name = "indexing"),
  @JsonSubTypes.Type(value = PurgingTask.class, name = "purging"),
  @JsonSubTypes.Type(value = ReceivingTask.class, name = "receiving"),
  @JsonSubTypes.Type(value = RemovingTask.class, name = "removing")
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
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant getStartTime();

  /**
   * Get the time when GeoRocket has finished executing the task. Note that
   * some tasks never finish because their end cannot be decided. In this case,
   * the method always returns {@code null}.
   * @return the task's end time (may be {@code null} if GeoRocket has not
   * finished the task yet or if the task's end cannot be decided)
   */
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant getEndTime();

  /**
   * Increment the values from this task by the values from the given one
   * @param other the task to merge into this one
   * @throws IllegalArgumentException if the given task is not compatible to
   * this one
   */
  void inc(Task other);

  /**
   * Get the errors that occurred during the execution of the task
   * @return a list of errors (may be {@code null} or empty if no errors have
   * occurred)
   */
  List<TaskError> getErrors();
}
