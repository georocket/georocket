package io.georocket.storage.mem;

import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.LocalMap;
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

  private LocalMap<String, Buffer> getLocalMap(Vertx vertx) {
    return vertx.sharedData().getLocalMap(MemoryStore.class.getName() + ".STORE");
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<String>> handler) {
    LocalMap<String, Buffer> lm = getLocalMap(vertx);
    String p = PathUtils.join(path, ID);
    lm.put(p, Buffer.buffer(CHUNK_CONTENT));
    handler.handle(Future.succeededFuture(p));
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<Void>> handler) {
    LocalMap<String, Buffer> lm = getLocalMap(vertx);
    context.assertEquals(1, lm.size());
    handler.handle(Future.succeededFuture());
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<Void>> handler) {
    LocalMap<String, Buffer> lm = getLocalMap(vertx);
    context.assertEquals(0, lm.size());
    handler.handle(Future.succeededFuture());
  }
}
