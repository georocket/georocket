package io.georocket.storage.h2;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

/**
 * Test {@link H2Store}
 * @author Michel Kraemer
 */
public class H2StoreTest extends StorageTest {
  /**
   * Create a temporary tempFolder
   */
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String path;
  private LegacyH2Store store;
  
  /**
   * Set up the test
   * @throws IOException if a temporary file could not be created
   */
  @Before
  public void setUp() throws IOException {
    path = tempFolder.newFile().getAbsolutePath();
  }
  
  /**
   * Release test resources
   */
  @After
  public void tearDown() {
    if (store != null) {
      store.close();
      store = null;
    }
  }
  
  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();
    config.put(ConfigConstants.STORAGE_H2_PATH, path);
  }

  @Override
  protected LegacyH2Store createStore(Vertx vertx) {
    if (store == null) {
      configureVertx(vertx);
      store = new LegacyH2Store(vertx);
    }
    return store;
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String path, Handler<AsyncResult<String>> handler) {
    String p = PathUtils.join(path, ID);
    createStore(vertx).getMap().put(p, CHUNK_CONTENT);
    handler.handle(Future.succeededFuture(p));
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx, String path,
    Handler<AsyncResult<Void>> handler) {
    context.assertEquals(1, createStore(vertx).getMap().size());
    handler.handle(Future.succeededFuture());
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context, Vertx vertx, String path,
    Handler<AsyncResult<Void>> handler) {
    context.assertEquals(0, createStore(vertx).getMap().size());
    handler.handle(Future.succeededFuture());
  }
}
