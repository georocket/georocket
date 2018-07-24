package io.georocket.storage.indexed;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.CursorInfo;
import io.georocket.storage.StoreCursor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * Implementation of {@link StoreCursor} for indexed chunk stores
 * @author Michel Kraemer
 */
public class IndexedStoreCursor implements StoreCursor {
  /**
   * The number of items retrieved in one batch
   */
  private static final int SIZE = 100;

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
   * The total number of items requested from the store
   */
  private Long totalHits = 0L;

  /**
   * The scrollId for elasticsearch
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
    new FrameCursor(vertx, search, path, SIZE).start(h -> {
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
    CursorInfo info = framedCursor.getInfo();
    this.totalHits = info.getTotalHits();
    this.scrollId = info.getScrollId();
  }

  @Override
  public boolean hasNext() {
    return pos + 1 < totalHits;
  }
  
  @Override
  public void next(Handler<AsyncResult<ChunkMeta>> handler) {
    ++pos;
    if (pos >= totalHits) {
      handler.handle(Future.failedFuture(new IndexOutOfBoundsException(
          "Cursor is beyond a valid position.")));
    } else if (this.currentFrameCursor.hasNext()) {
      this.currentFrameCursor.next(handler);
    } else {
      new FrameCursor(vertx, scrollId).start(h -> {
        if (h.failed()) {
          handler.handle(Future.failedFuture(h.cause()));
        } else {
          handleFrameCursor(h.result());
          this.currentFrameCursor.next(handler);
        }
      });
    }
  }

  @Override
  public String getChunkPath() {
    return this.currentFrameCursor.getChunkPath();
  }

  @Override
  public CursorInfo getInfo() {
    return this.currentFrameCursor.getInfo();
  }
}
