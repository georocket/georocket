package io.georocket.tasks;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for tasks
 * @author Michel Kraemer
 */
public abstract class AbstractTask implements Task {
  private static final int MAX_ERRORS = 10;

  private String correlationId;
  private Instant startTime;
  private Instant endTime;
  private List<TaskError> errors;

  /**
   * Package-visible default constructor
   */
  AbstractTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public AbstractTask(String correlationId) {
    this.correlationId = correlationId;
  }

  @Override
  public String getCorrelationId() {
    return correlationId;
  }

  /**
   * Package-visible setter for the task's correlation ID
   * @param correlationId the correlation ID
   */
  void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  @Override
  public Instant getStartTime() {
    return startTime;
  }

  /**
   * Set the task's start time
   * @param startTime the start time
   */
  public void setStartTime(Instant startTime) {
    this.startTime = startTime;
  }

  @Override
  public Instant getEndTime() {
    return endTime;
  }

  /**
   * Set the task's end time
   * @param endTime the end time
   */
  public void setEndTime(Instant endTime) {
    this.endTime = endTime;
  }

  @Override
  public List<TaskError> getErrors() {
    return errors;
  }

  /**
   * Set the list of errors that occurred during the task execution
   * @param errors the list errors (may be {@code null} or empty if no errors
   * have occurred)
   */
  public void setErrors(List<TaskError> errors) {
    this.errors = errors;

    if (this.errors.size() > MAX_ERRORS) {
      this.errors = this.errors.subList(0, MAX_ERRORS);
      addMoreErrors();
    }
  }

  /**
   * Add an error that occurred during the task execution
   * @param error the error to add
   */
  public void addError(TaskError error) {
    if (errors == null) {
      errors = new ArrayList<>();
    }
    if (errors.size() == MAX_ERRORS) {
      addMoreErrors();
    } else if (errors.size() < MAX_ERRORS) {
      errors.add(error);
    }
  }

  private void addMoreErrors() {
    errors.add(new TaskError("more", "There are more errors. Only " + MAX_ERRORS +
      " errors will be displayed. The server logs provide more information."));
  }

  @Override
  public void inc(Task other) {
    if (getStartTime() != null || other.getStartTime() != null) {
      if (getStartTime() != null && other.getStartTime() == null) {
        setStartTime(getStartTime());
      } else if (getStartTime() == null && other.getStartTime() != null) {
        setStartTime(other.getStartTime());
      } else if (getStartTime().isBefore(other.getStartTime())) {
        setStartTime(getStartTime());
      } else {
        setStartTime(other.getStartTime());
      }
    }

    if (getEndTime() != null || other.getEndTime() != null) {
      if (getEndTime() != null && other.getEndTime() == null) {
        setEndTime(getEndTime());
      } else if (getEndTime() == null && other.getEndTime() != null) {
        setEndTime(other.getEndTime());
      } else if (getEndTime().isBefore(other.getEndTime())) {
        setEndTime(getEndTime());
      } else {
        setEndTime(other.getEndTime());
      }
    }

    if (other.getErrors() != null) {
      other.getErrors().forEach(this::addError);
    }
  }
}
