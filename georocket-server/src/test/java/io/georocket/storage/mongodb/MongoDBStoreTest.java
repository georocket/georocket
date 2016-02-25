package io.georocket.storage.mongodb;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * Test {@link MongoDBStore}
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
public class MongoDBStoreTest {

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();


  private static String chunkContent = "<b>This is a chunk content</b>";
  private static String search = "irrelevant but necessary value"; // value is irrelevant for the test, because this test do not use the Indexer

  private static JsonObject indexerQueryReplyMsg;

  private static String id = new ObjectId().toString();
  private static JsonArray parents = new JsonArray();
  private static int start = 0;
  private static int end = 5;
  private static Long totalHits = 1L;
  private static String scrollId = "0";
  static {
    JsonArray hits = new JsonArray();
    JsonObject hit = new JsonObject()
        .put("parents", parents)
        .put("start", start)
        .put("end", end)
        .put("id", id);

    hits.add(hit);

    indexerQueryReplyMsg = new JsonObject()
        .put("totalHits", totalHits)
        .put("scrollId", scrollId)
        .put("hits", hits);
  }

  private MongoServer server;
  private InetSocketAddress serverAddress;
  private static String MONGODB_DBNAME = "testdb";

  @Before
  public void setUp() {
    server = new MongoServer(new MemoryBackend());
    serverAddress = server.bind();
  }

  @After
  public void tearDown() {
    server.shutdown();
  }

  private void write(TestContext context, String id, String content, String path) {
    MongoClient client = new MongoClient(new ServerAddress(serverAddress));
    DB db = client.getDB(MONGODB_DBNAME);

    GridFS gridFS = new GridFS(db);

    if (path == null || path.isEmpty()) {
      path = "/";
    }

    String filename = PathUtils.join(path, id);

    GridFSInputFile file = gridFS.createFile(filename);

    try(
        OutputStream os = file.getOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
    ) {
      writer.write(content);
    } catch (IOException ex) {
      context.fail("Test preparations failed: could not write a file to mongo db - " + ex.getMessage());
    }

    client.close();
  }


  private void setConfig(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    config.put(ConfigConstants.STORAGE_MONGODB_HOST, serverAddress.getHostName());
    config.put(ConfigConstants.STORAGE_MONGODB_PORT, serverAddress.getPort());
    config.put(ConfigConstants.STORAGE_MONGODB_DATABASE, MONGODB_DBNAME);
  }

  @Test
  public void testGetOne(TestContext context) throws Exception {
    Vertx vertx = rule.vertx();
    Async async = context.async();

    this.setConfig(vertx);

    this.write(context, id, chunkContent, null);

    Store store = new MongoDBStore(vertx);

    store.getOne(id, h -> {
      ChunkReadStream chunkReadStream = h.result();

      chunkReadStream.handler(buffer -> {
        String receivedChunk = new String(buffer.getBytes());
        context.assertEquals(chunkContent, receivedChunk);
      }).endHandler( end -> {
        async.complete();
      });
    });
  }

  @Test
  public void testAdd(TestContext context) throws Exception {
    context.fail("Test not implemented");
  }

  @Test
  public void testDelete(TestContext context) throws Exception {
    context.fail("Test not implemented");
  }

  @Test
  public void testGet(TestContext context) throws Exception {
    context.fail("Test not implemented");
  }
}