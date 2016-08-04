package io.georocket.constants;

/**
 * Constants for addresses on the event bus
 * @author Michel Kraemer
 */
@SuppressWarnings("javadoc")
public final class AddressConstants {
  public static final String GEOROCKET = "georocket";
  public static final String IMPORTER = "georocket.importer";
  public static final String INDEXER_ADD = "georocket.indexer.add";
  public static final String INDEXER_QUERY = "georocket.indexer.query";
  public static final String INDEXER_DELETE = "georocket.indexer.delete";

  public static final String STORE_ADD = "georocket.storage.add";
  public static final String STORE_GET_ONE = "georocket.storage.getone";
  public static final String STORE_DELETE = "georocket.storage.delete";
  public static final String STORE_GET = "georocket.storage.get";
  public static final String STORE_GET_SIZE = "georocket.storage.getsize";
  
  private AddressConstants() {
    // hidden constructor
  }
}
