package io.georocket.storage.mongodb;

import com.amazonaws.handlers.AsyncHandler;
import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.storage.StoreCursor;
import io.georocket.util.PathUtils;
import io.georocket.util.XMLStartElement;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.embeddedmongo.EmbeddedMongoVerticle;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.nio.charset.Charset;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Test {@link MongoDBStore}
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
public class MongoDBStoreTest {


  private static long MAX_WORKER_EXECUTION_TIME = 30 * 60 * 1000;
  @Rule
  public RunTestOnContext rule = new RunTestOnContext(new VertxOptions().setMaxWorkerExecuteTime(MAX_WORKER_EXECUTION_TIME));


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



  private static String MONGODB_DBNAME = "testdb";

  //private MongodExecutable exe;

  private InetSocketAddress serverAddress = new InetSocketAddress("localhost", 5005);


  @Before
  public void setUp(TestContext context) throws Exception {
    Vertx vertx = rule.vertx();

    vertx.getOrCreateContext().config().put("port", serverAddress.getPort());

    vertx.deployVerticle(EmbeddedMongoVerticle.class.getName(), new DeploymentOptions().setWorker(true).setConfig(vertx.getOrCreateContext().config()));

    /* Keep it until the decision was made which version should be kept
    IMongodConfig embeddedConfig = new MongodConfigBuilder().
        version(Version.Main.PRODUCTION).
        net(new Net(serverAddress.getPort(), Network.localhostIsIPv6())).
        build();

    exe = MongodStarter.getDefaultInstance().prepare(embeddedConfig);
    exe.start();
     */

  }

  @After
  public void tearDown() {
    //exe.stop();
  }

  private void write(TestContext context, Vertx vertx, String id, String content, String _path, Handler<AsyncResult<String>> handler) {
    vertx.<String>executeBlocking(bch -> {
      MongoClient client = new MongoClient(new ServerAddress(serverAddress));
      DB db = client.getDB(MONGODB_DBNAME);

      String path = _path;

      GridFS gridFS = new GridFS(db);

      if (path == null || path.isEmpty()) {
        path = "";
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

      bch.complete(filename);
    }, h -> handler.handle(Future.succeededFuture(h.result())));
  }

  private void read(TestContext context, Vertx vertx, String id, String _path, Handler<AsyncResult<String>> handler) {
    vertx.<String>executeBlocking(bch -> {
      MongoClient client = new MongoClient(new ServerAddress(serverAddress));
      DB db = client.getDB(MONGODB_DBNAME);
      GridFS gridFS = new GridFS(db);

      String path = _path;

      if (path == null || path.isEmpty()) {
        path = "";
      }

      String filename = PathUtils.join(path, id);

      GridFSDBFile file = gridFS.findOne(PathUtils.normalize(filename));

      String content = null;
      try {
        content = IOUtils.toString(file.getInputStream(), Charsets.UTF_8);
      } catch (IOException ex) {
        context.fail("Could not read the store: " + ex.getMessage());
      }

      bch.complete(content);
    }, h -> handler.handle(Future.succeededFuture(h.result())));
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

    this.write(context, vertx, id, chunkContent, null, f -> {

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
    });
  }

  @Test
  public void testAddWithoutPath(TestContext context) throws Exception {
    this.testAddHelper(context, null);
  }

  @Test
  public void testAddWithPath(TestContext context) throws Exception {
    this.testAddHelper(context, "bar");
  }

  public void testAddHelper(TestContext context, String path) throws Exception {
    String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    String xml = XMLHEADER + "<root>\n<object><child></child></object>\n</root>";
    ChunkMeta meta = new ChunkMeta(Arrays.asList(new XMLStartElement("root")), XMLHEADER.length() + 7, xml.length() - 8);
    List tags = Arrays.asList("a", "b", "c");

    Vertx vertx = rule.vertx();
    Async asyncIndexerAdd = context.async();
    Async asyncAdd = context.async();

    this.setConfig(vertx);

    Store store = new MongoDBStore(vertx);

    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(handler -> {
      JsonObject index = handler.body();

      context.assertEquals(meta.toJsonObject(), index.getJsonObject("meta"));
      context.assertEquals(new JsonArray(tags), index.getJsonArray("tags"));

      asyncIndexerAdd.complete();
    });

    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(h -> context.fail("Indexer should not be notified for delete on add of a store!"));
    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(h -> context.fail("Indexer should not be notified for query on add of a store!"));

    store.add(chunkContent, meta, path, tags, context.asyncAssertSuccess( err -> {

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

      asyncAdd.complete();
    }));
  }

  @Test
  public void testDeleteWithoutPath(TestContext context) throws Exception {
    this.testDeleteHelper(context, null);
  }

  @Test
  public void testDeleteWithSubfolder(TestContext context) throws Exception {
    this.testDeleteHelper(context, "bar");
  }

  private void testDeleteHelper(TestContext context, String _path) {
    Vertx vertx = rule.vertx();
    Async asyncIndexerQuery = context.async();
    Async asyncIndexerDelete = context.async();
    Async asyncDelete = context.async();

    this.write(context, vertx, id, chunkContent, _path, f -> {
      String path = f.result();

      this.setConfig(vertx);

      // register add
      vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> context.fail("Indexer should not be notified on delete of a store!"));
      // register delete
      vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(req -> {
        JsonObject msg = req.body();

        if(!msg.containsKey("paths")) context.fail("Malformed Message: expected to have 'pageSize' attribute");
        JsonArray paths = msg.getJsonArray("paths");

        if(paths.size() != 1) context.fail("Expected to find exact one path in MSG, found: " + paths.size());

        String notifiedPath = paths.getString(0);

        context.assertEquals(path, PathUtils.join(_path, notifiedPath));

        req.reply(null); // Value is not used in Store

        asyncIndexerDelete.complete();
      });

      // register query
      this.registerIndexerQueryConsumer(vertx, context, asyncIndexerQuery);


      Store store = new MongoDBStore(vertx);

      store.delete(search, path, context.asyncAssertSuccess(h -> {
        vertx.executeBlocking(k -> {
          MongoClient client = new MongoClient(new ServerAddress(serverAddress));
          DB db = client.getDB(MONGODB_DBNAME);
          GridFS gridFS = new GridFS(db);

          DBObject query = new BasicDBObject();

          List<GridFSDBFile> files = gridFS.find(query);

          if (!files.isEmpty()) context.fail("File with chunk's should be deleted on Storage::delete. Contains: " + files);

          k.complete();
        }, l -> asyncDelete.complete());
      }));
    });
  }

  private void registerIndexerQueryConsumer(Vertx vertx, TestContext context, Async async) {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(request -> {
      JsonObject msg = request.body();

      if (!msg.containsKey("pageSize")) context.fail("Malformed Message: expected to have 'pageSize' attribute");
      int pageSize = msg.getInteger("pageSize"); // pageSize == IndexStore.PAGE_SIZE | msg need this attribute

      if (!msg.containsKey("search")) context.fail("Malformed Message: expected to have 'search' attribute");
      String indxSearch = msg.getString("search");

      context.assertEquals(search, indxSearch);

      request.reply(indexerQueryReplyMsg);

      async.complete();
    });
  }

  @Test
  public void testGet(TestContext context) throws Exception {
    this.testGetHelper(context, null);
  }

  @Test
  public void testGetWithSubfolder(TestContext context) throws Exception {
    this.testGetHelper(context, "foo");
  }

  public void testGetHelper(TestContext context, String _path) throws Exception {
    Vertx vertx = rule.vertx();
    Async asyncQuery = context.async();
    Async asyncGet = context.async();

    // register query
    this.registerIndexerQueryConsumer(vertx, context, asyncQuery);

    // todo: prepare entity in mongodb
    this.write(context, vertx, id, chunkContent, _path, f -> {
      String path = f.result();

      this.setConfig(vertx);

      Store store = new MongoDBStore(vertx);

      store.get(search, path, ar -> {
        StoreCursor cursor = ar.result();

        if (!cursor.hasNext()) context.fail("Cursor is empty: Expected to have one element.");
        cursor.next(h -> {
          ChunkMeta meta = h.result();

          context.assertEquals(end, meta.getEnd());
          // context.assertEquals(parents, meta.getParents());
          context.assertEquals(start, meta.getStart());

          String fileName = cursor.getChunkPath();

          context.assertEquals(id, fileName);

          asyncGet.complete();
        });

      });
    });
  }
}