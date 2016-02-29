package io.georocket.storage;

import io.georocket.constants.AddressConstants;
import io.georocket.util.PathUtils;
import io.georocket.util.XMLStartElement;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

/**
 * Test {@link Store}
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
abstract public class StorageTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  protected static String chunkContent = "<b>This is a chunk content</b>";
  protected static String search = "irrelevant but necessary value"; // value is irrelevant for the test, because this test do not use the Indexer

  protected static JsonObject indexerQueryReplyMsg;

  protected static String id = new ObjectId().toString();
  protected static JsonArray parents = new JsonArray();
  protected static int start = 0;
  protected static int end = 5;
  protected static Long totalHits = 1L;
  protected static String scrollId = "0";
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

  protected abstract void configureVertx(Vertx vertx);
  protected abstract Store createStore(Vertx vertx);

  protected abstract Handler<Future<String>> prepare_Data(TestContext context, Vertx vertx, String path);
  protected abstract Handler<Future<Object>> validate_after_Store_add(TestContext context, Vertx vertx, String path);
  protected abstract Handler<Future<Object>> validate_after_Store_delete(TestContext context, Vertx vertx, String path);

  private void mockIndexer_Query(Vertx vertx, TestContext context, Async async) {
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

  // Test with path and without

  @Test
  public void testAddWithoutSubfolder(TestContext context) throws Exception {
    this.testAdd(context, null); // without path
  }

  @Test
  public void testAddWithSubfolder(TestContext context) throws Exception {
    this.testAdd(context, "subFolder"); // with path
  }

  @Test
  public void testDeleteWithoutSubfolder(TestContext context) throws Exception {
    this.testDelete(context, null);
  }

  @Test
  public void testDeleteWithSubfolder(TestContext context) throws Exception {
    this.testDelete(context, "subFolderA/subFolderB");
  }

  @Test
  public void testGetWithoutSubfolder(TestContext context) throws Exception {
    this.testGet(context, null);
  }

  @Test
  public void testGetWithSubfolder(TestContext context) throws Exception {
    this.testGet(context, "subFolder");
  }

  @Test
  public void testGetOneWithoutFolder(TestContext context) throws Exception {
    this.testGetOne(context, null);
  }

  @Test
  public void testGetOneWithSubfolder(TestContext context) throws Exception {
    this.testGetOne(context, "foo");
  }

  // Test implementations

  public void testGetOne(TestContext context, String _path) throws Exception {
    Vertx vertx = rule.vertx();
    Async async = context.async();

    this.configureVertx(vertx);

    vertx.<String>executeBlocking(
        this.prepare_Data(context, vertx, _path),
        fa -> {
          Store store = this.createStore(vertx);

          store.getOne(id, h -> {
            ChunkReadStream chunkReadStream = h.result();

            chunkReadStream.handler(buffer -> {
              String receivedChunk = new String(buffer.getBytes());
              context.assertEquals(chunkContent, receivedChunk);
              System.err.println("Done!");
            }).endHandler(end -> async.complete());
          });
        }
    );
  }

  protected void testAdd(TestContext context, String path) throws Exception {
    String xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    String xml = xmlHeader + "<root>\n<object><child></child></object>\n</root>";
    ChunkMeta meta = new ChunkMeta(Arrays.asList(new XMLStartElement("root")), xmlHeader.length() + 7, xml.length() - 8);
    List tags = Arrays.asList("a", "b", "c");

    Vertx vertx = rule.vertx();
    Async asyncIndexerAdd = context.async();
    Async asyncAdd = context.async();

    this.configureVertx(vertx);

    Store store = this.createStore(vertx);

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

    store.add(chunkContent, meta, path, tags, context.asyncAssertSuccess(err -> {
      vertx.executeBlocking(
          validate_after_Store_add(context, vertx, path),
          f -> asyncAdd.complete()
      );
    }));
  }

  public void testDelete(TestContext context, String _path) throws Exception {
    Vertx vertx = rule.vertx();
    Async asyncIndexerQuery = context.async();
    Async asyncIndexerDelete = context.async();
    Async asyncDelete = context.async();

    this.configureVertx(vertx);

    vertx.<String>executeBlocking(
        this.prepare_Data(context, vertx, _path),
        fa -> {
          String path = fa.result();

          Store store = this.createStore(vertx);

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
          this.mockIndexer_Query(vertx, context, asyncIndexerQuery);

          store.delete(search, path,
              context.asyncAssertSuccess(h ->
                  vertx.executeBlocking(
                      validate_after_Store_delete(context, vertx, path),
                      f -> asyncDelete.complete()
                  )
              )
          );
        }

    );
  }

  public void testGet(TestContext context, String _path) throws Exception {
    Vertx vertx = rule.vertx();
    Async asyncQuery = context.async();
    Async asyncGet = context.async();

    // register query
    this.mockIndexer_Query(vertx, context, asyncQuery);
    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(h -> context.fail("Indexer should not be notified for delete on get of a store!"));
    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> context.fail("Indexer should not be notified for add on get of a store!"));

    vertx.<String>executeBlocking(
        this.prepare_Data(context, vertx, _path),
        fa -> {
          String path = fa.result();

          this.configureVertx(vertx);

          Store store = this.createStore(vertx);

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
        }
    );
  }
}
