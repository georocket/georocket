package io.georocket.tasks;

/**
 * A task started by the {@link io.georocket.storage.indexed.IndexedStore} to
 * track the deletion of chunks from the store
 * @author Michel Kraemer
 */
public class PurgingTask extends AbstractTask {
  private int totalChunks;
  private int purgedChunks;

  /**
   * Package-visible default constructor
   */
  PurgingTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public PurgingTask(String correlationId) {
    super(correlationId);
  }

  /**
   * Get the total number of chunks to be purged by this task
   * @return the number of chunks to be purged
   */
  public int getTotalChunks() {
    return totalChunks;
  }

  /**
   * Set the total number of chunks to be purged by this task
   * @param totalChunks the total number of chunks to be purged
   */
  public void setTotalChunks(int totalChunks) {
    this.totalChunks = totalChunks;
  }

  /**
   * Get the number of chunks already purged by this task
   * @return the number of purged chunks
   */
  public int getPurgedChunks() {
    return purgedChunks;
  }

  /**
   * Set the number of chunks already purged by this task
   * @param purgedChunks the number of purged chunks
   */
  public void setPurgedChunks(int purgedChunks) {
    this.purgedChunks = purgedChunks;
  }

  @Override
  public void inc(Task other) {
    if (!(other instanceof PurgingTask)) {
      throw new IllegalArgumentException("Illegal task type");
    }
    PurgingTask po = (PurgingTask)other;
    super.inc(other);
    setPurgedChunks(getPurgedChunks() + po.getPurgedChunks());
    setTotalChunks(getTotalChunks() + po.getTotalChunks());
  }
}
