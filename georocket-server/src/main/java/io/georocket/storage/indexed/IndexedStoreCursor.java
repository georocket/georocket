package io.georocket.storage.indexed;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkMetaFactory;
import io.georocket.storage.StoreCursor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Implementation of {@link StoreCursor} for indexed chunk stores
 * @author Michel Kraemer
 */
public class IndexedStoreCursor implements StoreCursor {
  /**
   * The Vert.x instance
   */
  private final Vertx vertx;
  
  /**
   * The number of items retrieved in one batch
   */
  private final int pageSize;
  
  /**
   * The search query
   */
  private final String search;
  
  /**
   * The path where to perform the search (may be null)
   */
  private final String path;
  
  /**
   * The number of items retrieved from the store
   */
  private long count;
  
  /**
   * The current read position in {@link #ids} and {@link #metas}
   */
  private int pos = -1;
  
  /**
   * The total number of items requested from the store
   */
  private long size;
  
  /**
   * A scroll ID used by ElasticSearch for pagination
   */
  private String scrollId;
  
  /**
   * The chunk IDs retrieved in the last batch
   */
  private String[] ids;
  
  /**
   * Chunk metadata retrieved in the last batch
   */
  private ChunkMeta[] metas;
  
  /**
   * Create a cursor
   * @param vertx the Vert.x instance
   * @param pageSize the number of items retrieved in one batch
   * @param search the search query
   * @param path the path where to perform the search (may be null if the
   * whole store should be searched)
   */
  public IndexedStoreCursor(Vertx vertx, int pageSize, String search, String path) {
    this.vertx = vertx;
    this.pageSize = pageSize;
    this.search = search;
    this.path = path;
  }
  
  /**
   * Starts this cursor
   * @param handler will be called when the cursor has retrieved its first batch
   */
  public void start(Handler<AsyncResult<StoreCursor>> handler) {
    JsonObject queryMsg = new JsonObject()
        .put("pageSize", pageSize)
        .put("search", search);
    if (path != null) {
      queryMsg.put("path", path);
    }
    vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER_QUERY, queryMsg, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handleResponse(ar.result().body());
        handler.handle(Future.succeededFuture(this));
      }
    });
  }
  
  /**
   * Handle the response from the indexer and fill {@link #ids} and {@link #metas}
   * @param body the response from the indexer
   */
  private void handleResponse(JsonObject body) {
    size = body.getLong("totalHits");
    scrollId = body.getString("scrollId");
    JsonArray hits = body.getJsonArray("hits");
    int count = hits.size();
    ids = new String[count];
    metas = new ChunkMeta[count];
    for (int i = 0; i < count; ++i) {
      JsonObject hit = hits.getJsonObject(i);
      ids[i] = hit.getString("id");
      
      String metaType = hit.getString("$type");
      try {
        metas[i] = ChunkMetaFactory.createChunkMetaFromJson(metaType, hit);
      } catch (Exception e) {
        //
      }
    }
  }

  @Override
  public boolean hasNext() {
    return count < size;
  }

  @Override
  public void next(Handler<AsyncResult<ChunkMeta>> handler) {
    ++count;
    ++pos;
    if (pos >= metas.length) {
      JsonObject queryMsg = new JsonObject()
          .put("pageSize", pageSize)
          .put("search", search)
          .put("scrollId", scrollId);
      if (path != null) {
        queryMsg.put("path", path);
      }
      vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER_QUERY, queryMsg, ar -> {
        if (ar.failed()) {
          handler.handle(Future.failedFuture(ar.cause()));
        } else {
          handleResponse(ar.result().body());
          pos = 0;
          handler.handle(Future.succeededFuture(metas[pos]));
        }
      });
    } else {
      handler.handle(Future.succeededFuture(metas[pos]));
    }
  }
  
  @Override
  public String getChunkPath() {
    if (pos < 0) {
      throw new IllegalStateException("You have to call next() first");
    }
    return ids[pos];
  }
}
