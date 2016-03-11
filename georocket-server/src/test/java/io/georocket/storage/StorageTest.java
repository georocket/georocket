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
 * Abstract test implementation for a {@link Store}
 *
 * This class defines test methods for the store interface and should be used as base class for
 * all concrete Store tests.
 *
 * A concrete store test implement only the data preparation and some validation methods which have
 * access to the storage system.
 *
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
abstract public class StorageTest {

  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  /**
   * Test data: tempFolder name which is used to call the test with a folder
   */
  protected final static String testFolder = "testFolder";

  /**
   * Test data: content of a chunk
   */
  protected final static String chunkContent = "<b>This is a chunk content</b>";

  /**
   * Test data: search for a Store (value is irrelevant for the test, because this test do not use the Indexer)
   */
  protected final static String search = "irrelevant but necessary value";

  /**
   * Test data: version 1.0 XML standalone header
   */
  protected final static String xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";

  /**
   * Test data: a valid xml with header
   */
  protected final static String xml = xmlHeader + "<root>\n<object><child></child></object>\n</root>";

  /**
   * Test data: metadata for a chunk
   */
  protected final static ChunkMeta meta = new ChunkMeta(Arrays.asList(new XMLStartElement("root")), xmlHeader.length() + 7, xml.length() - 8);

  /**
   * Test data: a sample tag list for an Store::add method
   */
  protected final static List tags = Arrays.asList("a", "b", "c");

  /**
   * Test data: a randomly generated id for all tests.
   */
  protected final static String id = new ObjectId().toString();

  /**
   * Test data: the parents of one hit
   */
  protected final static JsonArray parents = new JsonArray();

  /**
   * Test data: start of a hit
   */
  protected final static int start = 0;

  /**
   * Test data: end of a hit
   */
  protected final static int end = 5;

  /**
   * Test data: Total amount of hits
   */
  protected final static Long totalHits = 1L;

  /**
   * Test data: scroll id
   */
  protected final static String scrollId = "0";


  /**
   * Create a JsonObject to simulate a reply from an indexer.
   *
   * @param path The path which is used as prefix of the id (@Nullable).
   *
   * @return A reply msg.
   */
  protected static JsonObject createIndexerQueryReply(String path) {

    if (path != null && !path.isEmpty()) {
      path = PathUtils.join(path, id);
    } else {
      path = id;
    }

    JsonArray hits = new JsonArray();
    JsonObject hit = new JsonObject()
        .put("parents", parents)
        .put("start", start)
        .put("end", end)
        .put("id", path);

    hits.add(hit);

    return new JsonObject()
        .put("totalHits", totalHits)
        .put("scrollId", scrollId)
        .put("hits", hits);
  }

  /**
   * Create a store implementation of a store which must be tested.
   *
   * @param vertx A vertx instance for one test
   *
   * @return A Store
   */
  protected abstract Store createStore(Vertx vertx);

  /**
   * Prepare test data for (every) test. Will be called during every test.
   *
   * Notice: Use the protected attributes as test data!
   * Call context.fail(...) if the data preparation failed!
   *
   * @param context The current test context.
   * @param vertx A vertx instance for one test.
   * @param path The path for the data (@Nullable).
   *
   * @return A Handler which will be called in a test, where test data are needed.
   */
  protected abstract Handler<Future<String>> prepare_Data(TestContext context, Vertx vertx, String path);

  /**
   * Validate the add method. Will be called after the store added data.
   *
   * Notice: Look on the protected attributes of this class to know which data were used for the store add method. These will be used for the @Store::add method.
   * Use context.assert ... and context.fail to validate the test.
   *
   * @param context The current test context.
   * @param vertx A vertx instance of one test.
   * @param path The path where the data where created (@Nullable: if not used for @prepare_Data)
   *
   * @return A Handler which will be called in a test where test data are needed.
   */
  protected abstract Handler<Future<Object>> validate_after_Store_add(TestContext context, Vertx vertx, String path);

  /**
   * Validate the delete method of a test. Will be called after the store delete's data.
   *
   * Notice: Look on the protected attributes and your @prepare_Data implementation to know which data you have deleted with the @Store::delete method.
   * Use context.assert ... and context.fail to validate the test.
   *
   * @param context The current test context.
   * @param vertx A vertx instance of one test.
   * @param path The path where the data were created (@Nullable: if not used for @prepare_Data)
   *
   * @return A Handler which will be called in a test where test data are needed.
   */
  protected abstract Handler<Future<Object>> validate_after_Store_delete(TestContext context, Vertx vertx, String path);

  private void mockIndexer_Query(Vertx vertx, TestContext context, Async async, String path) {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(request -> {
      JsonObject msg = request.body();

      if (!msg.containsKey("pageSize")) {
        context.fail("Malformed Message: expected to have 'pageSize' attribute");
      }

      int pageSize = msg.getInteger("pageSize"); // pageSize == IndexStore.PAGE_SIZE | msg need this attribute

      if (!msg.containsKey("search")) {
        context.fail("Malformed Message: expected to have 'search' attribute");
      }

      String indexSearch = msg.getString("search");

      context.assertEquals(search, indexSearch);

      request.reply(createIndexerQueryReply(path));

      async.complete();
    });
  }


  /*
   * Because the path is nullable all tests should be called with null as path and with a concrete path.
   */

  /**
   * Call @testAdd(context, null) with null as path.
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testAddWithoutSubfolder(TestContext context) throws Exception {
    this.testAdd(context, null);
  }

  /**
   * Call @testAdd(context, path) with a path.
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testAddWithSubfolder(TestContext context) throws Exception {
    this.testAdd(context, testFolder);
  }

  /**
   * Call @testDelete(context, null) with null as path.
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testDeleteWithoutSubfolder(TestContext context) throws Exception {
    this.testDelete(context, null);
  }

  /**
   * Call @testDelete(context, path) with a path.
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testDeleteWithSubfolder(TestContext context) throws Exception {
    this.testDelete(context, testFolder);
  }

  /**
   * Call @testGet(context, null) with null as path.
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testGetWithoutSubfolder(TestContext context) throws Exception {
    this.testGet(context, null);
  }

  /**
   * Call @testGet(context, path) with a path.
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testGetWithSubfolder(TestContext context) throws Exception {
    this.testGet(context, testFolder);
  }

  /**
   * Call @testGetOne(context, null) with null as path.
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testGetOneWithoutFolder(TestContext context) throws Exception {
    this.testGetOne(context, null);
  }

  /**
   * Apply the Store::delete with a not existing path and expects an success (no exceptions or failure codes).
   *
   * @param context Test context
   *
   * @throws Exception
   */
  @Test
  public void testDeleteNonExistingEntity(TestContext context) throws Exception {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    Async asyncIndexerQuery = context.async();

    Store store = this.createStore(vertx);

    // register add
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> context.fail("Indexer should not be notified for a add event after Store::delete was called!"));

    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(req -> context.fail("INDEXER_DELETE should not be notified if no file was found."));

    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(request -> {
      request.fail(404, "NOT FOUND");

      asyncIndexerQuery.complete();
    });

    store.delete(search, "NOT_EXISTING_PATH", context.asyncAssertSuccess(h -> {
      async.complete();
    }));
  }

  /**
   * Add test data to a storage and retrive the data with the @getOne method to compare them.
   *
   * Notice: uses the @prepare_Data
   *
   * @param context Test context
   * @param _path The path where to look for data (@Nullable).
   *
   * @throws Exception
   */
  public void testGetOne(TestContext context, String _path) throws Exception {
    Vertx vertx = rule.vertx();
    Async async = context.async();

    vertx.<String>executeBlocking(
        this.prepare_Data(context, vertx, _path),
        fa -> {
          Store store = this.createStore(vertx);

          store.getOne(id, context.asyncAssertSuccess(h -> {
            h.handler(buffer -> {
              String receivedChunk = new String(buffer.getBytes());
              context.assertEquals(chunkContent, receivedChunk);
            }).endHandler(end -> async.complete());
          }));
        }
    );
  }

  /**
   * Add test data and compare the data with the stored one.
   *
   * @param context Test context
   * @param path Path where to add data (@Nullable).
   *
   * @throws Exception
   */
  public void testAdd(TestContext context, String path) throws Exception {
    Vertx vertx = rule.vertx();
    Async asyncIndexerAdd = context.async();
    Async asyncAdd = context.async();

    Store store = this.createStore(vertx);

    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(handler -> {
      JsonObject index = handler.body();

      context.assertEquals(meta.toJsonObject(), index.getJsonObject("meta"));
      context.assertEquals(new JsonArray(tags), index.getJsonArray("tags"));

      asyncIndexerAdd.complete();
    });

    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(h -> context.fail("Indexer should not be notified for a delete event after Store::add was called!"));
    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(h -> context.fail("Indexer should not be notified for a query event after Store::add was called!"));

    store.add(chunkContent, meta, path, tags, context.asyncAssertSuccess(err -> {
      vertx.executeBlocking(
          validate_after_Store_add(context, vertx, path), // IO Operation => blocked call
          f -> asyncAdd.complete()
      );
    }));
  }

  /**
   * Add test data and try to delete them with the Store::delete method, then check the storage for any data.
   *
   * @param context Test context
   * @param _path Path where the data can be found (@Nullable).
   *
   * @throws Exception
   */
  public void testDelete(TestContext context, String _path) throws Exception {
    Vertx vertx = rule.vertx();
    Async asyncIndexerQuery = context.async();
    Async asyncIndexerDelete = context.async();
    Async asyncDelete = context.async();

    vertx.<String>executeBlocking(
        this.prepare_Data(context, vertx, _path), // IO Operation => blocked call
        fa -> {
          String path = fa.result();

          Store store = this.createStore(vertx);

          // register add
          vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> context.fail("Indexer should not be notified for a add event after Store::delete was called!"));
          // register delete
          vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(req -> {
            JsonObject msg = req.body();

            if(!msg.containsKey("paths")) {
              context.fail("Malformed Message: expected to have 'pageSize' attribute");
            }

            JsonArray paths = msg.getJsonArray("paths");

            if(paths.size() != 1) {
              context.fail("Expected to find exact one path in message, found: " + paths.size());
            }

            String notifiedPath = paths.getString(0);

            context.assertEquals(path, notifiedPath);

            req.reply(null); // Value is not used in Store

            asyncIndexerDelete.complete();
          });

          // register query
          this.mockIndexer_Query(vertx, context, asyncIndexerQuery, _path);


          store.delete(search, _path,
              context.asyncAssertSuccess(h ->
                  vertx.executeBlocking(
                      validate_after_Store_delete(context, vertx, path), // IO Operation => blocked call
                      f -> asyncDelete.complete()
                  )
              )
          );
        }

    );
  }

  /**
   * Add test data with meta data and try to retrieve them with the Storage::testGet method
   *
   * @param context Test context.
   * @param _path The path where the data can be found (@Nullable).
   *
   * @throws Exception
   */
  public void testGet(TestContext context, String _path) throws Exception {
    Vertx vertx = rule.vertx();
    Async asyncQuery = context.async();
    Async asyncGet = context.async();

    // register query
    this.mockIndexer_Query(vertx, context, asyncQuery, _path);
    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(h -> context.fail("Indexer should not be notified for a delete event after Store::get was called!"));
    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> context.fail("Indexer should not be notified for a add event after Store::get was called!"));

    vertx.<String>executeBlocking(
        this.prepare_Data(context, vertx, _path), // IO Operation => blocked call
        fa -> {
          String path = fa.result();

          Store store = this.createStore(vertx);

          store.get(search, path, ar -> {
            StoreCursor cursor = ar.result();

            if (!cursor.hasNext()) {
              context.fail("Cursor is empty: Expected one element.");
            }

            cursor.next(h -> {
              ChunkMeta meta = h.result();

              context.assertEquals(end, meta.getEnd());
              // context.assertEquals(parents, meta.getParents());
              context.assertEquals(start, meta.getStart());

              String fileName = cursor.getChunkPath();

              context.assertEquals(path == null || path.isEmpty() ? id : PathUtils.join(_path, id), fileName);

              asyncGet.complete();
            });
          });
        }
    );
  }
}
