package io.georocket.tasks;

/**
 * A task started by the {@link io.georocket.ImporterVerticle}
 * @author Michel Kraemer
 */
public class ImporterTask extends AbstractTask {
  private int importedChunks;

  /**
   * Package-visible default constructor
   */
  ImporterTask() {
    // nothing to do here
  }

  /**
   * Default constructor
   * @param correlationId the correlation ID this task belongs to
   */
  public ImporterTask(String correlationId) {
    super(correlationId);
  }

  /**
   * Get the number of chunks already imported by this task
   * @return the number of imported chunks
   */
  public int getImportedChunks() {
    return importedChunks;
  }

  /**
   * Set the number of chunks already imported by this task
   * @param importedChunks the number of imported chunks
   */
  public void setImportedChunks(int importedChunks) {
    this.importedChunks = importedChunks;
  }

  /**
   * Increase the number of chunks already imported by this task by 1
   */
  public void incImportedChunks() {
    incImportedChunks(1);
  }

  /**
   * Increase the number of chunks already imported by this task by the
   * given number
   * @param inc the increment
   */
  public void incImportedChunks(int inc) {
    importedChunks += inc;
  }

  @Override
  public void inc(Task other) {
    if (!(other instanceof ImporterTask)) {
      throw new IllegalArgumentException("Illegal task type");
    }
    ImporterTask io = (ImporterTask)other;
    super.inc(other);
    setImportedChunks(getImportedChunks() + io.getImportedChunks());
  }
}
