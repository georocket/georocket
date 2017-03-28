package io.georocket.storage.indexed;

import static io.georocket.util.MimeTypeUtils.belongsTo;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.storage.JsonChunkMeta;
import io.georocket.storage.PaginatedStoreCursor;
import io.georocket.storage.StoreCursor;
import io.georocket.storage.XMLChunkMeta;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Implementation of {@link StoreCursor} for indexed chunk stores
 * 
 * @author Michel Kraemer
 */
public class IndexedStoreCursor implements PaginatedStoreCursor {
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
  protected long size;

  /**
   * A scroll ID used by Elasticsearch for pagination
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

  private int isPaginated;

  /**
   * Create a cursor
   * 
   * @param vertx
   *          the Vert.x instance
   * @param pageSize
   *          the number of items retrieved in one batch
   * @param search
   *          the search query
   * @param path
   *          the path where to perform the search (may be null if the whole
   *          store should be searched)
   */
  public IndexedStoreCursor(Vertx vertx, int pageSize, String search, String path) {
    this(vertx, pageSize, search, path, null, false);
  }

  public IndexedStoreCursor(Vertx vertx, int pageSize, String search, String path, String scrollId, Boolean paginated) {
    this.vertx = vertx;
    this.pageSize = pageSize;
    this.search = search;
    this.path = path;
    this.scrollId = scrollId;
    // TODO
    this.isPaginated = paginated ? 999 : 1;
  }
  
  /**
   * Starts this cursor
   * 
   * @param handler
   *          will be called when the cursor has retrieved its first batch
   */
  public void start(Handler<AsyncResult<StoreCursor>> handler) {
    JsonObject queryMsg = new JsonObject()
      .put("pageSize", pageSize)
      .put("search", search);

    if (scrollId != null) {
      queryMsg.put("scrollId", scrollId);
    }

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
   * Handle the response from the indexer and fill {@link #ids} and
   * {@link #metas}
   * 
   * @param body
   *          the response from the indexer
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
      metas[i] = createChunkMeta(hit);
    }
  }

  @Override
  public boolean hasNext() {
    return count < size  && isPaginated > 0;
  }

  @Override
  public void next(Handler<AsyncResult<ChunkMeta>> handler) {
    ++count;
    ++pos;
    if (pos >= metas.length) {
      --isPaginated;
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

  /**
   * Create a {@link XMLChunkMeta} object. Sub-classes may override this method
   * to provide their own {@link XMLChunkMeta} type.
   * 
   * @param hit
   *          the chunk meta content used to initialize the object
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

  @Override
  public JsonObject getPaginationInfo() {
    return new JsonObject()
        .put("scrollId", scrollId)
        .put("totalHits", size)
        .put("hits", count);
  }
}
