package io.georocket.storage.mongodb;

import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.embeddedmongo.EmbeddedMongoVerticle;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Test {@link MongoDBStore}
 * @author Andrej Sajenko
 */
public class MongoDBStoreTest extends StorageTest {
  private static long MAX_WORKER_EXECUTION_TIME = 30 * 60 * 1000;

  public MongoDBStoreTest() {
    super.rule = new RunTestOnContext(new VertxOptions().setMaxWorkerExecuteTime(MAX_WORKER_EXECUTION_TIME));
  }

  private static String MONGODB_DBNAME = "testdb";
  private InetSocketAddress serverAddress = new InetSocketAddress("localhost", 5005);

  /**
   * Set up test dependencies.
   */
  @Before
  public void setUp() {
    Vertx vertx = rule.vertx();

    vertx.getOrCreateContext().config().put("port", serverAddress.getPort());

    vertx.deployVerticle(
        EmbeddedMongoVerticle.class.getName(),
        new DeploymentOptions()
            .setWorker(true)
            .setConfig(vertx.getOrCreateContext().config())
    );
  }

  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    config.put(ConfigConstants.STORAGE_MONGODB_HOST, serverAddress.getHostName());
    config.put(ConfigConstants.STORAGE_MONGODB_PORT, serverAddress.getPort());
    config.put(ConfigConstants.STORAGE_MONGODB_DATABASE, MONGODB_DBNAME);
  }

  @Override
  protected Store createStore(Vertx vertx) {
    this.configureVertx(vertx);
    return new MongoDBStore(vertx);
  }

  @Override
  protected Handler<Future<String>> prepare_Data(TestContext context, Vertx vertx, String _path) {
    return h -> {
      MongoClient client = new MongoClient(new ServerAddress(serverAddress));
      DB db = client.getDB(MONGODB_DBNAME);

      String path = _path;

      GridFS gridFS = new GridFS(db);

      if (path == null || path.isEmpty()) {
        path = "";
      }

      String filename = PathUtils.join(path, id);

      GridFSInputFile file = gridFS.createFile(filename);

      try (
          OutputStream os = file.getOutputStream();
          OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
      ) {
        writer.write(chunkContent);
      } catch (IOException ex) {
        context.fail("Test preparations failed: could not write a file to mongo db - " + ex.getMessage());
      }

      client.close();

      h.complete(filename);
    };
  }

  @Override
  protected Handler<Future<Object>> validate_after_Store_add(TestContext context, Vertx vertx, String path) {
    return h -> {
      MongoClient client = new MongoClient(new ServerAddress(serverAddress));
      DB db = client.getDB(MONGODB_DBNAME);
      GridFS gridFS = new GridFS(db);

      DBObject query = new BasicDBObject();

      List<GridFSDBFile> files = gridFS.find(query);

      if (files.isEmpty()) {
        context.fail("MongoDB did not save a entity");
      }

      GridFSDBFile file = files.get(0);
      InputStream is = file.getInputStream();

      String content = null;
      try {
        content = IOUtils.toString(is, Charsets.UTF_8);
      } catch (IOException ex) {
        context.fail("Could not read GridDSDBFile: " + ex.getMessage());
      }

      context.assertEquals(chunkContent, content);

      client.close();

      h.complete();
    };
  }

  @Override
  protected Handler<Future<Object>> validate_after_Store_delete(TestContext context, Vertx vertx, String path) {
    return h -> {
      MongoClient client = new MongoClient(new ServerAddress(serverAddress));
      DB db = client.getDB(MONGODB_DBNAME);
      GridFS gridFS = new GridFS(db);

      DBObject query = new BasicDBObject();

      List<GridFSDBFile> files = gridFS.find(query);

      if (!files.isEmpty()) {
        context.fail("File with chunks should be deleted on Storage::delete. Contains: " + files);
      }

      client.close();

      h.complete();
    };
  }
}
