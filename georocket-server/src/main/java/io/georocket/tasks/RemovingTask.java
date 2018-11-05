package io.georocket.tasks;

/**
 * A task started by the {@link io.georocket.index.IndexerVerticle} to track the
 * removal of chunks from the index
 * @author Michel Kraemer
 */
public class RemovingTask extends AbstractTask {
  private long totalChunks;
  private long removedChunks;

  /**
   * Package-visible default constructor
   */
  RemovingTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public RemovingTask(String correlationId) {
    super(correlationId);
  }

  /**
   * Get the total number of chunks to be removed by this task
   * @return the number of chunks to be removed
   */
  public long getTotalChunks() {
    return totalChunks;
  }

  /**
   * Set the total number of chunks to be removed by this task
   * @param totalChunks the total number of chunks to be removed
   */
  public void setTotalChunks(long totalChunks) {
    this.totalChunks = totalChunks;
  }

  /**
   * Get the number of chunks already removed by this task
   * @return the number of removed chunks
   */
  public long getRemovedChunks() {
    return removedChunks;
  }

  /**
   * Set the number of chunks already removed by this task
   * @param removedChunks the number of removed chunks
   */
  public void setRemovedChunks(long removedChunks) {
    this.removedChunks = removedChunks;
  }

  @Override
  public void inc(Task other) {
    if (!(other instanceof RemovingTask)) {
      throw new IllegalArgumentException("Illegal task type");
    }
    RemovingTask ro = (RemovingTask)other;
    super.inc(other);
    setRemovedChunks(getRemovedChunks() + ro.getRemovedChunks());
    setTotalChunks(Math.max(getTotalChunks(), ro.getTotalChunks()));
  }
}
