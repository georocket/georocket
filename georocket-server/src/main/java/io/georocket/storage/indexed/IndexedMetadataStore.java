package io.georocket.storage.indexed;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.AsyncCursor;
import io.georocket.storage.MetadataStore;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;
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

  @Override
  public void setProperties(String search, String path,
    Map<String, String> properties, Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("properties", JsonObject.mapFrom(properties));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_SET_PROPERTIES, msg, handler);
  }

  @Override
  public void removeProperties(String search, String path,
    List<String> properties, Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("properties", new JsonArray(properties));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_REMOVE_PROPERTIES, msg, handler);
  }

  @Override
  public void appendTags(String search, String path, List<String> tags,
    Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("tags", new JsonArray(tags));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_APPEND_TAGS, msg, handler);
  }

  @Override
  public void removeTags(String search, String path, List<String> tags,
    Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("tags", new JsonArray(tags));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_REMOVE_TAGS, msg, handler);
  }

  /**
   * Send a message to the specified address and pass null to the handler when
   * the verticle responds
   * @param address the address to send it to
   * @param msg the message to send
   * @param handler the handler which is called when the verticle responds
   */
  private void send(String address, Object msg,
    Handler<AsyncResult<Void>> handler) {
    vertx.eventBus().send(address, msg, ar -> handler.handle(ar.map(x -> null)));
  }
}
