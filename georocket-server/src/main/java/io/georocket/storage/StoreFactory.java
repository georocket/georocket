package io.georocket.storage;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.file.FileStore;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * A factory for chunk stores
 * @author Michel Kraemer
 */
public class StoreFactory {
  /**
   * Create the configured chunk store
   * @param vertx the Vert.x instance
   * @return the store
   */
  public static Store createStore(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();
    String cls = config.getString(ConfigConstants.STORAGE_CLASS,
        FileStore.class.getName());
    try {
      return (Store)Class.forName(cls).getConstructor(Vertx.class).newInstance(vertx);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not create chunk store", e);
    }
  }
}
