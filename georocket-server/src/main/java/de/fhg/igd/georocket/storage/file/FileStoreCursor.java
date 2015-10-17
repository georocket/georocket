package de.fhg.igd.georocket.storage.file;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.storage.ChunkReadStream;
import de.fhg.igd.georocket.storage.StoreCursor;
import de.fhg.igd.georocket.util.ChunkMeta;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Implementation of {@link StoreCursor} for the {@link FileStore}
 * @author Michel Kraemer
 */
public class FileStoreCursor implements StoreCursor {
  /**
   * The Vert.x instance
   */
  private final Vertx vertx;
  
  /**
   * The store we're iterating over
   */
  private final FileStore store;
  
  /**
   * The number of items retrieved in one batch
   */
  private final int pageSize;
  
  /**
   * The number of items retrieved from the store
   */
  private long i;
  
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
   * @param store the store we're iterating over
   * @param pageSize the number of items retrieved in one batch
   */
  public FileStoreCursor(Vertx vertx, FileStore store, int pageSize) {
    this.vertx = vertx;
    this.store = store;
    this.pageSize = pageSize;
  }
  
  /**
   * Starts this cursor
   * @param handler will be called when the cursor has retrieved its first batch
   */
  public void start(Handler<AsyncResult<StoreCursor>> handler) {
    JsonObject queryMsg = new JsonObject()
        .put("action", "query")
        .put("pageSize", pageSize);
    vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, queryMsg, ar -> {
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
      metas[i] = ChunkMeta.fromJsonObject(hit);
    }
  }

  @Override
  public boolean hasNext() {
    return i < size;
  }

  @Override
  public void next(Handler<AsyncResult<ChunkMeta>> handler) {
    int p = (int)(i % pageSize);
    ++i;
    if (i > 1 && p == 0) {
      JsonObject queryMsg = new JsonObject()
          .put("action", "query")
          .put("pageSize", pageSize)
          .put("scrollId", scrollId);
      vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, queryMsg, ar -> {
        if (ar.failed()) {
          handler.handle(Future.failedFuture(ar.cause()));
        } else {
          handleResponse(ar.result().body());
          handler.handle(Future.succeededFuture(metas[p]));
        }
      });
    } else {
      handler.handle(Future.succeededFuture(metas[p]));
    }
  }
  
  @Override
  public String getChunkId() {
    if (i == 0) {
      throw new IllegalStateException("You have to call next() first");
    }
    
    int p = (int)((i - 1) % pageSize);
    return ids[p];
  }

  @Override
  public void openChunk(Handler<AsyncResult<ChunkReadStream>> handler) {
    store.getOne(getChunkId(), handler);
  }
}
