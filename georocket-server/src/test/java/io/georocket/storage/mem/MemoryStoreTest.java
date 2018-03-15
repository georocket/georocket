package io.georocket.storage.mem;

import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.unit.TestContext;

/**
 * Test {@link MemoryStore}
 * @author Michel Kraemer
 */
public class MemoryStoreTest extends StorageTest {
  @Override
  protected Store createStore(Vertx vertx) {
    return new MemoryStore(vertx);
  }

  private void getAsyncMap(Vertx vertx,
      Handler<AsyncResult<AsyncMap<String, Buffer>>> handler) {
    String name = MemoryStore.class.getName() + ".STORE";
    vertx.sharedData().getAsyncMap(name, handler);
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<String>> handler) {
    getAsyncMap(vertx, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      AsyncMap<String, Buffer> am = ar.result();
      String p = PathUtils.join(path, ID);
      am.put(p, Buffer.buffer(CHUNK_CONTENT), par -> {
        if (par.failed()) {
          handler.handle(Future.failedFuture(par.cause()));
        } else {
          handler.handle(Future.succeededFuture(p));
        }
      });
    });
  }
  
  private void assertSize(TestContext context, Vertx vertx, int expectedSize,
      Handler<AsyncResult<Void>> handler) {
    getAsyncMap(vertx, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      AsyncMap<String, Buffer> am = ar.result();
      am.size(sar -> {
        if (sar.failed()) {
          handler.handle(Future.failedFuture(sar.cause()));
        } else {
          context.assertEquals(expectedSize, sar.result());
          handler.handle(Future.succeededFuture());
        }
      });
    });
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context,
      Vertx vertx, String path, Handler<AsyncResult<Void>> handler) {
    assertSize(context, vertx, 1, handler);
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context,
      Vertx vertx, String path, Handler<AsyncResult<Void>> handler) {
    assertSize(context, vertx, 0, handler);
  }
}
