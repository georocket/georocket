package io.georocket.storage.indexed;

import static io.georocket.util.MimeTypeUtils.belongsTo;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.CursorInfo;
import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.storage.JsonChunkMeta;
import io.georocket.storage.StoreCursor;
import io.georocket.storage.XMLChunkMeta;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * A cursor to run over a subset of data.
 * @author Andrej Sajenko
 */
public class FrameCursor implements StoreCursor {
  /**
   * The Vert.x instance
   */
  private final Vertx vertx;

  /**
   * The current read position in {@link #ids} and {@link #metas}
   * <p>INV: <code>pos + 1 < metas.length</code></p>
   */
  private int pos = -1;

  /**
   * A scroll ID used by Elasticsearch for pagination
   */
  private String scrollId;

  /**
   * The search query
   */
  private String search;
  
  /**
   * The path where to perform the search (may be null)
   */
  private String path;

  /**
   * The size of elements to load in the frame.
   * <p>INV: <code>size == metas.length</code></p>
   */
  private int size;

  /**
   * The total number of items the store has to offer.
   * If {@link #size} > totalHits then this frame cursor will
   * never load all items of the store.
   * 
   * <b>INV: <code>SIZE <= totalHits</code></b>
   */
  private long totalHits;

  /**
   * The chunk IDs retrieved in the last batch
   */
  private String[] ids;

  /**
   * Chunk metadata retrieved in the last batch
   */
  private ChunkMeta metas[];

  /**
   * Load the first frame of chunks.
   * @param vertx vertx instance
   * @param search The search query
   * @param path The search path
   * @param size The number of elements to load in this frame
   */
  public FrameCursor(Vertx vertx, String search, String path, int size) {
    this(vertx, null);
    this.search = search;
    this.path = path;
    this.size = size;
  }

  /**
   * Load the next frame with a scrollId.
   * Use {@link #getInfo()} to get the scrollId.
   * @param vertx vertx instance
   * @param scrollId scrollId to load the next frame
   */
  public FrameCursor(Vertx vertx, String scrollId) {
    this.vertx = vertx;
    this.scrollId = scrollId;
  }

  /**
   * Starts this cursor
   * @param handler will be called when the cursor has retrieved its first batch
   */
  public void start(Handler<AsyncResult<StoreCursor>> handler) {
    JsonObject queryMsg = new JsonObject();

    if (scrollId != null) {
      queryMsg.put("scrollId", scrollId);
    } else {
      queryMsg
        .put("size", size)
        .put("search", search);
      if (path != null) {
        queryMsg.put("path", path);
      }
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

  @Override
  public boolean hasNext() {
    return pos + 1 < metas.length;
  }

  @Override
  public void next(Handler<AsyncResult<ChunkMeta>> handler) {
    vertx.runOnContext(v -> {
      ++pos;
      if (pos >= metas.length) {
        handler.handle(Future.failedFuture(new IndexOutOfBoundsException("Cursor out of bound.")));
      } else {
        handler.handle(Future.succeededFuture(metas[pos]));
      }
    });
  }

  @Override
  public String getChunkPath() {
    if (pos < 0) {
      throw new IllegalStateException("You have to call next() first");
    }
    return ids[pos];
  }

  @Override
  public CursorInfo getInfo() {
    return new CursorInfo(scrollId, totalHits, metas.length);
  }

  /**
   * Handle the response from the indexer.
   * @param body The indexer response body.
   */
  protected void handleResponse(JsonObject body) {
    totalHits = body.getLong("totalHits");
    scrollId = body.getString("scrollId");
    JsonArray hits = body.getJsonArray("hits");
    int count = hits.size();
    ids = new String[count];
    metas = new ChunkMeta[count];
    for (int i = 0; i < count; ++i) {
      JsonObject hit = hits.getJsonObject(i);
      ids[i] = hit.getString("id");
      metas[i] = createChunkMeta(hit);
    }
  }

  /**
   * Create a {@link XMLChunkMeta} object. Sub-classes may override this
   * method to provide their own {@link XMLChunkMeta} type.
   * @param hit the chunk meta content used to initialize the object
   * @return the created object
   */
  protected ChunkMeta createChunkMeta(JsonObject hit) {
    String mimeType = hit.getString("mimeType", XMLChunkMeta.MIME_TYPE);
    if (belongsTo(mimeType, "application", "xml") ||
      belongsTo(mimeType, "text", "xml")) {
      return new XMLChunkMeta(hit);
    } else if (belongsTo(mimeType, "application", "geo+json")) {
      return new GeoJsonChunkMeta(hit);
    } else if (belongsTo(mimeType, "application", "json")) {
      return new JsonChunkMeta(hit);
    }
    return new ChunkMeta(hit);
  }
}
