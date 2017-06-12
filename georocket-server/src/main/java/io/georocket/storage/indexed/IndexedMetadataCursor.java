package io.georocket.storage.indexed;

import io.georocket.storage.AsyncCursor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of {@link AsyncCursor} for {@link IndexedMetadataStore}
 * @author Tim Hellhake
 * @param <T> type of the item
 */
public class IndexedMetadataCursor<T> implements AsyncCursor<T> {
  /**
   * A function which knows how to decode the items
   */
  private final Function<Object, T> itemDecoder;

  /**
   * The eventbus address of the item type
   */
  private final String address;

  /**
   * The Vert.x instance
   */
  private final Vertx vertx;

  /**
   * The number of items retrieved in one batch
   */
  private final int pageSize;

  /**
   * A template of the message which should be used to query items
   */
  private final JsonObject template;

  /**
   * The number of items retrieved from the store
   */
  private long count;

  /**
   * The current read position in {@link #items}
   */
  private int pos = -1;

  /**
   * The total number of items requested from the store
   */
  private long size;

  /**
   * A scroll ID used by Elasticsearch for pagination
   */
  private String scrollId;

  /**
   * Chunk metadata retrieved in the last batch
   */
  private List<T> items;

  /**
   * Create a cursor
   * @param itemDecoder a function which knows how to decode the items
   * @param address The eventbus address of the item type
   * @param vertx the Vert.x instance
   * @param pageSize the number of items retrieved in one batch
   * @param template a template of the message which should be used to query items
   */
  public IndexedMetadataCursor(Function<Object, T> itemDecoder, String address,
    Vertx vertx, int pageSize, JsonObject template) {
    this.itemDecoder = itemDecoder;
    this.address = address;
    this.vertx = vertx;
    this.pageSize = pageSize;
    this.template = template;
  }

  /**
   * Starts this cursor
   * @param handler will be called when the cursor has retrieved its first batch
   */
  public void start(Handler<AsyncResult<AsyncCursor<T>>> handler) {
    JsonObject queryMsg = template.copy()
      .put("pageSize", pageSize);
    vertx.eventBus().<JsonObject>send(address, queryMsg, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handleResponse(ar.result().body());
        handler.handle(Future.succeededFuture(this));
      }
    });
  }

  /**
   * Handle the response from the metadata verticle and set the items list
   * @param body the response from the indexer
   */
  private void handleResponse(JsonObject body) {
    size = body.getLong("totalHits");
    scrollId = body.getString("scrollId");
    JsonArray hits = body.getJsonArray("hits");
    items = hits.stream()
      .map(itemDecoder)
      .collect(Collectors.toList());
  }

  @Override
  public boolean hasNext() {
    return count < size;
  }

  @Override
  public void next(Handler<AsyncResult<T>> handler) {
    ++count;
    ++pos;
    if (pos >= items.size()) {
      JsonObject queryMsg = template.copy()
        .put("pageSize", pageSize)
        .put("scrollId", scrollId);
      vertx.eventBus().<JsonObject>send(address, queryMsg, ar -> {
        if (ar.failed()) {
          handler.handle(Future.failedFuture(ar.cause()));
        } else {
          handleResponse(ar.result().body());
          pos = 0;
          handler.handle(Future.succeededFuture(items.get(pos)));
        }
      });
    } else {
      handler.handle(Future.succeededFuture(items.get(pos)));
    }
  }
}
