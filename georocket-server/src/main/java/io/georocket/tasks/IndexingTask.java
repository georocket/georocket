package io.georocket.tasks;

/**
 * A task started by the {@link io.georocket.index.IndexerVerticle}
 * @author Michel Kraemer
 */
public class IndexingTask extends AbstractTask {
  private long indexedChunks;

  /**
   * Package-visible default constructor
   */
  IndexingTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public IndexingTask(String correlationId) {
    super(correlationId);
  }

  /**
   * Get the number of chunks already indexed by this task
   * @return the number of indexed chunks
   */
  public long getIndexedChunks() {
    return indexedChunks;
  }

  /**
   * Set the number of chunks already indexed by this task
   * @param indexedChunks the number of indexed chunks
   */
  public void setIndexedChunks(long indexedChunks) {
    this.indexedChunks = indexedChunks;
  }

  @Override
  public void inc(Task other) {
    if (!(other instanceof IndexingTask)) {
      throw new IllegalArgumentException("Illegal task type");
    }
    IndexingTask io = (IndexingTask)other;
    super.inc(other);
    setIndexedChunks(getIndexedChunks() + io.getIndexedChunks());
  }
}
