package io.georocket.storage;

/**
 * A metadata object containing additional information about a deletion process
 * @since 1.4.0
 * @author Michel Kraemer
 */
public class DeleteMeta {
  private final String correlationId;

  /**
   * Default constructor
   * @param correlationId identifies to which specific deletion process this
   * meta data belongs
   */
  public DeleteMeta(String correlationId) {
    this.correlationId = correlationId;
  }

  /**
   * @return an ID that specifies to which specific deletion process this meta
   * data belongs
   */
  public String getCorrelationId() {
    return correlationId;
  }
}
