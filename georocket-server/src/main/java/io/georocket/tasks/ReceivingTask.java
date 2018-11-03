package io.georocket.tasks;

/**
 * A task started by the {@link io.georocket.http.StoreEndpoint} when it
 * receives a file from the client
 * @author Michel Kraemer
 */
public class ReceivingTask extends AbstractTask {
  /**
   * Package-visible default constructor
   */
  ReceivingTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public ReceivingTask(String correlationId) {
    super(correlationId);
  }
}
