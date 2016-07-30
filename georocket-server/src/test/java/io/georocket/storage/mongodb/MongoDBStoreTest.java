package io.georocket.storage.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;

/**
 * Test {@link MongoDBStore}
 * @author Andrej Sajenko
 */
public class MongoDBStoreTest extends StorageTest {
  private static long MAX_WORKER_EXECUTION_TIME = 30 * 60 * 1000;
  
  private MongoDBTestConnector mongoConnector;

  /**
   * Default constructor
   */
  public MongoDBStoreTest() {
    super.rule = new RunTestOnContext(new VertxOptions()
        .setMaxWorkerExecuteTime(MAX_WORKER_EXECUTION_TIME));
  }

  /**
   * Set up test dependencies.
   * @throws IOException if the MongoDB instance could not be started
   */
  @Before
  public void setUp() throws IOException {
    mongoConnector = new MongoDBTestConnector();
  }

  /**
   * Uninitialize tests
   */
  @After
  public void tearDown() {
    mongoConnector.stop();
  }

  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    config.put(ConfigConstants.STORAGE_MONGODB_HOST,
        mongoConnector.serverAddress.getHost());
    config.put(ConfigConstants.STORAGE_MONGODB_PORT,
        mongoConnector.serverAddress.getPort());
    config.put(ConfigConstants.STORAGE_MONGODB_DATABASE,
        MongoDBTestConnector.MONGODB_DBNAME);
  }

  @Override
  protected Store createStore(Vertx vertx) {
    configureVertx(vertx);
    return new MongoDBStore(vertx);
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<String>> handler) {
    String filename = PathUtils.join(path, ID);
    vertx.<String>executeBlocking(f -> {
      try (MongoClient client = new MongoClient(mongoConnector.serverAddress)) {
        DB db = client.getDB(MongoDBTestConnector.MONGODB_DBNAME);
        GridFS gridFS = new GridFS(db);
        GridFSInputFile file = gridFS.createFile(filename);
        try (
          OutputStream os = file.getOutputStream();
          OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
        ) {
          writer.write(CHUNK_CONTENT);
          f.complete(filename);
        }
      } catch (IOException ex) {
        f.fail(ex);
      }
    }, handler);
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      MongoClient client = new MongoClient(mongoConnector.serverAddress);
      DB db = client.getDB(MongoDBTestConnector.MONGODB_DBNAME);
      GridFS gridFS = new GridFS(db);

      DBObject query = new BasicDBObject();

      List<GridFSDBFile> files = gridFS.find(query);
      context.assertFalse(files.isEmpty());

      GridFSDBFile file = files.get(0);
      InputStream is = file.getInputStream();

      String content = null;
      try {
        content = IOUtils.toString(is, Charsets.UTF_8);
      } catch (IOException ex) {
        context.fail("Could not read GridDSDBFile: " + ex.getMessage());
      }

      context.assertEquals(CHUNK_CONTENT, content);

      client.close();

      f.complete();
    }, handler);
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      MongoClient client = new MongoClient(mongoConnector.serverAddress);
      DB db = client.getDB(MongoDBTestConnector.MONGODB_DBNAME);
      GridFS gridFS = new GridFS(db);

      DBObject query = new BasicDBObject();

      List<GridFSDBFile> files = gridFS.find(query);
      context.assertTrue(files.isEmpty());

      client.close();

      f.complete();
    }, handler);
  }
}
