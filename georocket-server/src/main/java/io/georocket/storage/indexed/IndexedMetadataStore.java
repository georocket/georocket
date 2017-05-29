package io.georocket.storage.indexed;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.AsyncCursor;
import io.georocket.storage.MetadataStore;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

/**
 * An store which manages the access to indexed metadata
 * @author Tim Hellhake
 */
public class IndexedMetadataStore implements MetadataStore {
  private static final int PAGE_SIZE = 100;
  private final Vertx vertx;

  /**
   * Constructs the metadata store
   * @param vertx the Vert.x instance
   */
  public IndexedMetadataStore(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void getPropertyValues(String search, String path, String property,
    Handler<AsyncResult<AsyncCursor<String>>> handler) {
    JsonObject template = new JsonObject()
      .put("search", search)
      .put("property", property);
    if (path != null) {
      template.put("path", path);
    }
    new IndexedMetadataCursor<>(Objects::toString,
      AddressConstants.METADATA_GET_PROPERTIES, vertx, PAGE_SIZE, template)
      .start(handler);
  }
}
