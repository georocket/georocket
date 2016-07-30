package io.georocket.storage.mongodb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.collect.Iterables;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;

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
  
  private static MongoDBTestConnector mongoConnector;

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
  @BeforeClass
  public static void setUpClass() throws IOException {
    mongoConnector = new MongoDBTestConnector();
  }

  /**
   * Uninitialize tests
   */
  @AfterClass
  public static void tearDownClass() {
    mongoConnector.stop();
    mongoConnector = null;
  }
  
  /**
   * Uninitialize tests
   */
  @After
  public void tearDown() {
    try (MongoClient client = new MongoClient(mongoConnector.serverAddress)) {
      MongoDatabase db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME);
      db.drop();
    }
  }

  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    config.put(ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING,
        "mongodb://" + mongoConnector.serverAddress.getHost() + ":" +
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
        MongoDatabase db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME);
        GridFSBucket gridFS = GridFSBuckets.create(db);
        byte[] contents = CHUNK_CONTENT.getBytes(StandardCharsets.UTF_8);
        gridFS.uploadFromStream(filename, new ByteArrayInputStream(contents));
        f.complete(filename);
      }
    }, handler);
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      try (MongoClient client = new MongoClient(mongoConnector.serverAddress)) {
        MongoDatabase db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME);
        GridFSBucket gridFS = GridFSBuckets.create(db);
  
        GridFSFindIterable files = gridFS.find();
  
        GridFSFile file = files.first();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        gridFS.downloadToStream(file.getFilename(), baos);
        String contents = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        context.assertEquals(CHUNK_CONTENT, contents);
      }
      f.complete();
    }, handler);
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      try (MongoClient client = new MongoClient(mongoConnector.serverAddress)) {
        MongoDatabase db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME);
        GridFSBucket gridFS = GridFSBuckets.create(db);
  
        GridFSFindIterable files = gridFS.find();
        context.assertTrue(Iterables.isEmpty(files));
      }
      f.complete();
    }, handler);
  }
}
