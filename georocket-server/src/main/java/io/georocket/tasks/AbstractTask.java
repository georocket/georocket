package io.georocket.tasks;

import java.util.Calendar;

/**
 * Abstract base class for tasks
 * @author Michel Kraemer
 */
public abstract class AbstractTask implements Task {
  private String correlationId;
  private Calendar startTime;
  private Calendar endTime;

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
  public Calendar getStartTime() {
    return startTime;
  }

  /**
   * Set the task's start time
   * @param startTime the start time
   */
  public void setStartTime(Calendar startTime) {
    this.startTime = startTime;
  }

  @Override
  public Calendar getEndTime() {
    return endTime;
  }

  /**
   * Set the task's end time
   * @param endTime the end time
   */
  public void setEndTime(Calendar endTime) {
    this.endTime = endTime;
  }

  @Override
  public void inc(Task other) {
    if (getStartTime() != null || other.getStartTime() != null) {
      if (getStartTime() != null && other.getStartTime() == null) {
        setStartTime(getStartTime());
      } else if (getStartTime() == null && other.getStartTime() != null) {
        setStartTime(other.getStartTime());
      } else if (getStartTime().before(other.getStartTime())) {
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
      } else if (getEndTime().before(other.getEndTime())) {
        setEndTime(getEndTime());
      } else {
        setEndTime(other.getEndTime());
      }
    }
  }
}
