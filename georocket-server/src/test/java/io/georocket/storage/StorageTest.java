package io.georocket.storage;

import java.util.Arrays;
import java.util.List;

import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

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

/**
 * <p>Abstract test implementation for a {@link Store}</p>
 *
 * <p>This class defines test methods for the store interface and should be
 * used as base class for all concrete Store tests.</p>
 *
 * <p>A concrete store test implement only the data preparation and some
 * validation methods which have access to the storage system.</p>
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
  protected final static String TEST_FOLDER = "testFolder";

  /**
   * Test data: content of a chunk
   */
  protected final static String CHUNK_CONTENT = "<b>This is a chunk content</b>";

  /**
   * Test data: search for a Store (value is irrelevant for the test, because
   * this test do not use the Indexer)
   */
  protected final static String SEARCH = "irrelevant but necessary value";

  /**
   * Test data: version 1.0 XML standalone header
   */
  protected final static String XML_HEADER =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";

  /**
   * Test data: a valid xml with header
   */
  protected final static String XML =
      XML_HEADER + "<root>\n<object><child></child></object>\n</root>";

  /**
   * Test data: metadata for a chunk
   */
  protected final static ChunkMeta META =
      new ChunkMeta(Arrays.asList(new XMLStartElement("root")),
          XML_HEADER.length() + 7, XML.length() - 8);

  /**
   * Test data: a sample tag list for an Store::add method
   */
  protected final static List<String> TAGS = Arrays.asList("a", "b", "c");

  /**
   * Test data: a randomly generated id for all tests.
   */
  protected final static String ID = new ObjectId().toString();

  /**
   * Test data: the parents of one hit
   */
  protected final static JsonArray PARENTS = new JsonArray();

  /**
   * Test data: start of a hit
   */
  protected final static int START = 0;

  /**
   * Test data: end of a hit
   */
  protected final static int END = 5;

  /**
   * Test data: Total amount of hits
   */
  protected final static Long TOTAL_HITS = 1L;

  /**
   * Test data: scroll id
   */
  protected final static String SCROLL_ID = "0";

  /**
   * Create a JsonObject to simulate a reply from an indexer
   * @param path The path which is used as prefix of the id (may be null)
   * @return A reply msg
   */
  protected static JsonObject createIndexerQueryReply(String path) {
    if (path != null && !path.isEmpty()) {
      path = PathUtils.join(path, ID);
    } else {
      path = ID;
    }

    JsonArray hits = new JsonArray();
    JsonObject hit = new JsonObject()
        .put("parents", PARENTS)
        .put("start", START)
        .put("end", END)
        .put("id", path);

    hits.add(hit);

    return new JsonObject()
        .put("totalHits", TOTAL_HITS)
        .put("scrollId", SCROLL_ID)
        .put("hits", hits);
  }

  /**
   * Create the store under test
   * @param vertx A Vert.x instance for one test
   * @return A Store
   */
  protected abstract Store createStore(Vertx vertx);

  /**
   * <p>Prepare test data for (every) test. Will be called during every test.</p>
   * <p>Heads up: use the protected attributes as test data!
   * Call context.fail(...) if the data preparation failed!</p>
   * @param context The current test context.
   * @param vertx A Vert.x instance for one test.
   * @param path The path for the data (may be null).
   * @return A Handler which will be called in a test, where test data are needed.
   */
  protected abstract Handler<Future<String>> prepareData(TestContext context,
      Vertx vertx, String path);

  /**
   * <p>Validate the add method. Will be called after the store added data.</p>
   * <p>Heads up: look on the protected attributes of this class to know which
   * data were used for the store add method. These will be used for the
   * {@link Store#add(String, ChunkMeta, String, List, Handler)} method.
   * Use context.assert ... and context.fail to validate the test.</p>
   * @param context The current test context.
   * @param vertx A Vert.x instance of one test.
   * @param path The path where the data was created (may be null if not used
   * for {@link #prepareData(TestContext, Vertx, String)})
   * @return A Handler which will be called in a test where test data are needed.
   */
  protected abstract Handler<Future<Object>> validateAfterStoreAdd(
      TestContext context, Vertx vertx, String path);

  /**
   * <p>Validate the delete method of a test. Will be called after the store
   * deleted data.</p>
   * <p>Heads up: look on the protected attributes and your
   * {@link #prepareData(TestContext, Vertx, String)} implementation to know
   * which data you have deleted with the
   * {@link Store#delete(String, String, Handler)} method.
   * Use context.assert ... and context.fail to validate the test.</p>
   * @param context The current test context.
   * @param vertx A Vert.x instance of one test.
   * @param path The path where the data were created (may be null if not used
   * for {@link #prepareData(TestContext, Vertx, String)})
   * @return A Handler which will be called in a test where test data are needed.
   */
  protected abstract Handler<Future<Object>> validateAfterStoreDelete(
      TestContext context, Vertx vertx, String path);

  private void mockIndexerQuery(Vertx vertx, TestContext context, Async async, String path) {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(request -> {
      JsonObject msg = request.body();

      if (!msg.containsKey("pageSize")) {
        context.fail("Malformed Message: expected to have 'pageSize' attribute");
      }

      if (!msg.containsKey("search")) {
        context.fail("Malformed Message: expected to have 'search' attribute");
      }

      String indexSearch = msg.getString("search");

      context.assertEquals(SEARCH, indexSearch);

      request.reply(createIndexerQueryReply(path));

      async.complete();
    });
  }

  /**
   * Call {@link #testAdd(TestContext, String)} with null as path.
   * @param context Test context
   */
  @Test
  public void testAddWithoutSubfolder(TestContext context) {
    this.testAdd(context, null);
  }

  /**
   * Call {@link #testAdd(TestContext, String)} with a path.
   * @param context Test context
   */
  @Test
  public void testAddWithSubfolder(TestContext context) {
    this.testAdd(context, TEST_FOLDER);
  }

  /**
   * Call {@link #testDelete(TestContext, String)} with null as path.
   * @param context Test context
   */
  @Test
  public void testDeleteWithoutSubfolder(TestContext context) {
    this.testDelete(context, null);
  }

  /**
   * Call {@link #testDelete(TestContext, String)} with a path.
   * @param context Test context
   */
  @Test
  public void testDeleteWithSubfolder(TestContext context) {
    this.testDelete(context, TEST_FOLDER);
  }

  /**
   * Call {@link #testGet(TestContext, String)} with null as path.
   * @param context Test context
   */
  @Test
  public void testGetWithoutSubfolder(TestContext context) {
    this.testGet(context, null);
  }

  /**
   * Call {@link #testGet(TestContext, String)} with a path.
   * @param context Test context
   */
  @Test
  public void testGetWithSubfolder(TestContext context) {
    this.testGet(context, TEST_FOLDER);
  }

  /**
   * Call {@link #testGetOne(TestContext, String)} with null as path.
   * @param context Test context
   */
  @Test
  public void testGetOneWithoutFolder(TestContext context) {
    this.testGetOne(context, null);
  }

  /**
   * Apply the {@link Store#delete(String, String, Handler)} with a
   * non-existing path and expects an success (no exceptions or failure codes).
   * @param context Test context
   */
  @Test
  @Ignore("Expected to fail at the moment")
  public void testDeleteNonExistingEntity(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    Async asyncIndexerQuery = context.async();

    Store store = this.createStore(vertx);

    // register add
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h ->
      context.fail("Indexer should not be notified for a add event after "
          + "Store::delete was called!"));

    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(r ->
      context.fail("INDEXER_DELETE should not be notified if no file was found."));

    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(r -> {
      r.fail(404, "NOT FOUND");
      asyncIndexerQuery.complete();
    });

    store.delete(SEARCH, "NOT_EXISTING_PATH", context.asyncAssertSuccess(h -> {
      async.complete();
    }));
  }

  /**
   * <p>Add test data to a storage and retrieve the data with the
   * {@link Store#getOne(String, Handler)} method to compare them.</p>
   * <p>Heads up: uses the {@link #prepareData(TestContext, Vertx, String)}</p>
   * @param context Test context
   * @param path The path where to look for data (may be null)
   */
  public void testGetOne(TestContext context, String path) {
    Vertx vertx = rule.vertx();
    Async async = context.async();

    vertx.<String>executeBlocking(
        this.prepareData(context, vertx, path),
        fa -> {
          Store store = this.createStore(vertx);

          store.getOne(ID, context.asyncAssertSuccess(h -> {
            h.handler(buffer -> {
              String receivedChunk = new String(buffer.getBytes());
              context.assertEquals(CHUNK_CONTENT, receivedChunk);
            }).endHandler(end -> async.complete());
          }));
        }
    );
  }

  /**
   * Add test data and compare the data with the stored one
   * @param context Test context
   * @param path Path where to add data (may be null)
   */
  public void testAdd(TestContext context, String path) {
    Vertx vertx = rule.vertx();
    Async asyncIndexerAdd = context.async();
    Async asyncAdd = context.async();

    Store store = this.createStore(vertx);

    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> {
      JsonObject index = h.body();

      context.assertEquals(META.toJsonObject(), index.getJsonObject("meta"));
      context.assertEquals(new JsonArray(TAGS), index.getJsonArray("tags"));

      asyncIndexerAdd.complete();
    });

    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(h ->
      context.fail("Indexer should not be notified for a delete event after"
          + "Store::add was called!"));
    
    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(h ->
      context.fail("Indexer should not be notified for a query event after"
          + "Store::add was called!"));

    store.add(CHUNK_CONTENT, META, path, TAGS, context.asyncAssertSuccess(err -> {
      vertx.executeBlocking(
          validateAfterStoreAdd(context, vertx, path), // IO Operation => blocked call
          f -> asyncAdd.complete()
      );
    }));
  }

  /**
   * Add test data and try to delete them with the 
   * {@link Store#delete(String, String, Handler)} method, then check the
   * storage for any data.
   * @param context Test context
   * @param path Path where the data can be found (may be null).
   */
  public void testDelete(TestContext context, String path) {
    Vertx vertx = rule.vertx();
    Async asyncIndexerQuery = context.async();
    Async asyncIndexerDelete = context.async();
    Async asyncDelete = context.async();

    vertx.<String>executeBlocking(
        this.prepareData(context, vertx, path), // IO Operation => blocked call
        fa -> {
          String resultPath = fa.result();

          Store store = this.createStore(vertx);

          // register add
          vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h ->
            context.fail("Indexer should not be notified for a add event after"
                + "Store::delete was called!"));
          
          // register delete
          vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(req -> {
            JsonObject msg = req.body();

            if (!msg.containsKey("paths")) {
              context.fail("Malformed Message: expected to have 'pageSize' attribute");
            }

            JsonArray paths = msg.getJsonArray("paths");

            if (paths.size() != 1) {
              context.fail("Expected to find exact one path in message, found: " + paths.size());
            }

            String notifiedPath = paths.getString(0);

            context.assertEquals(resultPath, notifiedPath);

            req.reply(null); // Value is not used in Store

            asyncIndexerDelete.complete();
          });

          // register query
          this.mockIndexerQuery(vertx, context, asyncIndexerQuery, path);

          store.delete(SEARCH, path,
              context.asyncAssertSuccess(h ->
                  vertx.executeBlocking(
                      validateAfterStoreDelete(context, vertx, resultPath), // IO Operation => blocked call
                      f -> asyncDelete.complete()
                  )
              )
          );
        }
    );
  }

  /**
   * Add test data with meta data and try to retrieve them with the
   * {@link Store#get(String, String, Handler)} method
   * @param context Test context.
   * @param path The path where the data can be found (may be null).
   */
  public void testGet(TestContext context, String path) {
    Vertx vertx = rule.vertx();
    Async asyncQuery = context.async();
    Async asyncGet = context.async();

    // register query
    this.mockIndexerQuery(vertx, context, asyncQuery, path);
    
    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(h ->
      context.fail("Indexer should not be notified for a delete event after"
          + "Store::get was called!"));
    
    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h ->
      context.fail("Indexer should not be notified for an add event after"
          + "Store::get was called!"));

    vertx.<String>executeBlocking(
        this.prepareData(context, vertx, path), // IO Operation => blocked call
        fa -> {
          String resultPath = fa.result();

          Store store = this.createStore(vertx);

          store.get(SEARCH, resultPath, ar -> {
            StoreCursor cursor = ar.result();

            if (!cursor.hasNext()) {
              context.fail("Cursor is empty: Expected one element.");
            }

            cursor.next(h -> {
              ChunkMeta meta = h.result();

              context.assertEquals(END, meta.getEnd());
              // context.assertEquals(parents, meta.getParents());
              context.assertEquals(START, meta.getStart());

              String fileName = cursor.getChunkPath();

              context.assertEquals(resultPath == null || resultPath.isEmpty() ?
                  ID : PathUtils.join(path, ID), fileName);

              asyncGet.complete();
            });
          });
        }
    );
  }
}
