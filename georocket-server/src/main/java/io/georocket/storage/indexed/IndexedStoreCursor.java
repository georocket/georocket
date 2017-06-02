package io.georocket.storage.indexed;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.FrameInfo;
import io.georocket.storage.StoreCursor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
   * The search query
   */
  private final String search;

  /**
   * The path where to perform the search (may be null)
   */
  private final String path;

  /**
   * This cursor use FrameCursor to load the full datastore frame by frame.
   */
  private StoreCursor currentFrameCursor; 

  /**
   * The current read position
   */
  private int pos = -1;

  /**
   * The position in the the current frame.
   */
  private Integer posInFrame = -1;

  /**
   * The total number of items requested from the store
   */
  private Long totalHits = 0L;

  /**
   * The numver of items received in the current frame.
   */
  private Integer currentHits = 0;

  /**
   * A scroll ID used by Elasticsearch for pagination
   */
  private String scrollId;

  /**
   * Create a cursor
   * @param vertx the Vert.x instance
   * @param search the search query
   * @param path the path where to perform the search (may be null if the
   * whole store should be searched)
   */
  public IndexedStoreCursor(Vertx vertx, String search, String path) {
    this.vertx = vertx;
    this.search = search;
    this.path = path;
  }

  /**
   * Starts this cursor
   * @param handler will be called when the cursor has retrieved its first batch
   */
  public void start(Handler<AsyncResult<StoreCursor>> handler) {
    new FrameCursor(vertx, search, path).start(h -> {
      if (h.succeeded()) {
        handleFrameCursor(h.result());
        handler.handle(Future.succeededFuture(this));
      } else {
        handler.handle(Future.failedFuture(h.cause()));
      }
    });
  }
  
  private void handleFrameCursor(StoreCursor framedCursor) {
    currentFrameCursor = framedCursor;
    FrameInfo info = framedCursor.getInfo();
    this.totalHits = info.getTotalHits();
    this.currentHits = info.getCurrentHits();
    this.scrollId = info.getScrollId();
  }

  @Override
  public boolean hasNext() {
    return pos + 1 < totalHits;
  }

  
  @Override
  public void next(Handler<AsyncResult<ChunkMeta>> handler) {
    ++posInFrame;
    ++pos;
    if (pos >= totalHits) {
      handler.handle(Future.failedFuture(new IndexOutOfBoundsException("Curser is out of a valid position.")));
    } else if (posInFrame >= currentHits) {
      posInFrame = - 1;
      new FrameCursor(vertx, scrollId).start(h -> {
        if (h.failed()) {
          handler.handle(Future.failedFuture(h.cause()));
        } else {
          handleFrameCursor(h.result());
          this.currentFrameCursor.next(handler);
        }
      });
    } else {
      this.currentFrameCursor.next(handler);
    }
  }

  @Override
  public String getChunkPath() {
    return this.currentFrameCursor.getChunkPath();
  }

  @Override
  public FrameInfo getInfo() {
    return this.currentFrameCursor.getInfo();
  }
}
