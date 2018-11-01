package io.georocket.storage.mem;

import java.io.FileNotFoundException;
import java.util.Queue;

import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.AsyncMap;

/**
 * <p>Stores chunks in memory</p>
 * <p><strong>Attention: The store is not persisted. Contents will be lost when
 * GeoRocket quits.</strong></p>
 * @author Michel Kraemer
 */
public class MemoryStore extends IndexedStore implements Store {
  private final Vertx vertx;
  private AsyncMap<String, Buffer> store;
  
  /**
   * Default constructor
   * @param vertx the Vert.x instance
   */
  public MemoryStore(Vertx vertx) {
    super(vertx);
    this.vertx = vertx;
  }
  
  private void getStore(Handler<AsyncResult<AsyncMap<String, Buffer>>> handler) {
    if (store != null) {
      handler.handle(Future.succeededFuture(store));
      return;
    }
    
    String name = getClass().getName() + ".STORE";
    vertx.sharedData().<String, Buffer>getAsyncMap(name, ar -> {
      if (ar.succeeded()) {
        store = ar.result();
      }
      handler.handle(ar);
    });
  }
  
  @Override
  protected void doAddChunk(String chunk, String path, String correlationId,
      Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    String filename = PathUtils.join(path, generateChunkId(correlationId));
    
    getStore(ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        ar.result().put(filename, Buffer.buffer(chunk), par -> {
          if (par.failed()) {
            handler.handle(Future.failedFuture(par.cause()));
          } else {
            handler.handle(Future.succeededFuture(filename));
          }
        });
      }
    });
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    String finalPath = PathUtils.normalize(path);
    getStore(ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        ar.result().get(finalPath, gar -> {
          if (gar.failed()) {
            handler.handle(Future.failedFuture(gar.cause()));
          } else {
            Buffer chunk =  gar.result();
            if (chunk == null) {
              handler.handle(Future.failedFuture(new FileNotFoundException(
                "Could not find chunk: " + finalPath)));
              return;
            }
            handler.handle(Future.succeededFuture(new DelegateChunkReadStream(chunk)));
          }
        });
      }
    });
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    getStore(ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        doDeleteChunks(paths, ar.result(), handler);
      }
    });
  }
  
  private void doDeleteChunks(Queue<String> paths, AsyncMap<String, Buffer> store,
      Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }
    
    String path = PathUtils.normalize(paths.poll());
    store.remove(path, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        doDeleteChunks(paths, store, handler);
      }
    });
  }
}
