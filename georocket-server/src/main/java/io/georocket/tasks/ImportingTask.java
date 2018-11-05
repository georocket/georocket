package io.georocket.tasks;

/**
 * A task started by the {@link io.georocket.ImporterVerticle}
 * @author Michel Kraemer
 */
public class ImportingTask extends AbstractTask {
  private long importedChunks;

  /**
   * Package-visible default constructor
   */
  ImportingTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public ImportingTask(String correlationId) {
    super(correlationId);
  }

  /**
   * Get the number of chunks already imported by this task
   * @return the number of imported chunks
   */
  public long getImportedChunks() {
    return importedChunks;
  }

  /**
   * Set the number of chunks already imported by this task
   * @param importedChunks the number of imported chunks
   */
  public void setImportedChunks(long importedChunks) {
    this.importedChunks = importedChunks;
  }

  @Override
  public void inc(Task other) {
    if (!(other instanceof ImportingTask)) {
      throw new IllegalArgumentException("Illegal task type");
    }
    ImportingTask io = (ImportingTask)other;
    super.inc(other);
    setImportedChunks(getImportedChunks() + io.getImportedChunks());
  }
}
