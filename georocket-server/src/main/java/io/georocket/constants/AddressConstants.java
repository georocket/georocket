package io.georocket.constants;

/**
 * Constants for addresses on the event bus
 * @author Michel Kraemer
 */
@SuppressWarnings("javadoc")
public final class AddressConstants {
  public static final String GEOROCKET = "georocket";
  public static final String IMPORTER_IMPORT = "georocket.importer.import";
  public static final String IMPORTER_PAUSE = "georocket.importer.pause";
  public static final String INDEXER_ADD = "georocket.indexer.add";
  public static final String INDEXER_QUERY = "georocket.indexer.query";
  public static final String INDEXER_DELETE = "georocket.indexer.delete";
  public static final String METADATA_GET_ATTRIBUTE_VALUES = "georocket.metadata.attribute.values.get";
  public static final String METADATA_GET_PROPERTY_VALUES = "georocket.metadata.property.values.get";
  public static final String METADATA_SET_PROPERTIES = "georocket.metadata.properties.set";
  public static final String METADATA_REMOVE_PROPERTIES = "georocket.metadata.properties.remove";
  public static final String METADATA_APPEND_TAGS = "georocket.metadata.tags.append";
  public static final String METADATA_REMOVE_TAGS = "georocket.metadata.tags.remove";
  public static final String TASK_GET_ALL = "georocket.task.getAll";
  public static final String TASK_GET_BY_CORRELATION_ID = "georocket.task.getByCorrelationId";
  public static final String TASK_INC = "georocket.task.inc";

  private AddressConstants() {
    // hidden constructor
  }
}
