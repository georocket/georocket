package io.georocket.tasks;

/**
 * A task started by the {@link io.georocket.index.IndexerVerticle}
 * @author Michel Kraemer
 */
public class IndexerTask extends AbstractTask {
  private int indexedChunks;

  /**
   * Package-visible default constructor
   */
  IndexerTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public IndexerTask(String correlationId) {
    super(correlationId);
  }

  /**
   * Get the number of chunks already indexed by this task
   * @return the number of indexed chunks
   */
  public int getIndexedChunks() {
    return indexedChunks;
  }

  /**
   * Set the number of chunks already indexed by this task
   * @param indexedChunks the number of indexed chunks
   */
  public void setIndexedChunks(int indexedChunks) {
    this.indexedChunks = indexedChunks;
  }

  /**
   * Increase the number of chunks already indexed by this task by 1
   */
  public void incIndexedChunks() {
    incIndexedChunks(1);
  }

  /**
   * Increase the number of chunks already indexed by this task by the
   * given number
   * @param inc the increment
   */
  public void incIndexedChunks(int inc) {
    indexedChunks += inc;
  }

  @Override
  public void inc(Task other) {
    if (!(other instanceof IndexerTask)) {
      throw new IllegalArgumentException("Illegal task type");
    }
    IndexerTask io = (IndexerTask)other;
    super.inc(other);
    setIndexedChunks(getIndexedChunks() + io.getIndexedChunks());
  }
}
