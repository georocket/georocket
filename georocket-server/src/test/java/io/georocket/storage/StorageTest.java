package io.georocket.storage;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableMap;

import io.georocket.constants.AddressConstants;
import io.georocket.util.PathUtils;
import io.georocket.util.XMLStartElement;
import io.vertx.core.AsyncResult;
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
  protected final static XMLChunkMeta META =
      new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
          XML_HEADER.length() + 7, XML.length() - 8);

  /**
   * Test data: fallback CRS for chunk indexing
   */
  protected final static String FALLBACK_CRS_STRING = "EPSG:25832";

  /**
   * Test data: the import id of a file import
   */
  protected final static String IMPORT_ID = "Af023dasd3";

  /**
   * Test data: the timestamp for an import
   */
  protected final static long TIMESTAMP = System.currentTimeMillis();

  /**
   * Test data: a sample tag list for an Store::add method
   */
  protected final static List<String> TAGS = Arrays.asList("a", "b", "c");

  /**
   * Test data: a sample property map for an Store::add method
   */
  protected final static Map<String, Object> PROPERTIES = ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3");

  /**
   * Test data: a randomly generated id for all tests.
   */
  protected final static String ID = new ObjectId().toString();

  /**
   * Test data: path to a non existing entity
   */
  protected final static String PATH_TO_NON_EXISTING_ENTITY = ID;

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
   * @param handler will be called when the test data has been prepared
   */
  protected abstract void prepareData(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<String>> handler);

  /**
   * <p>Validate the add method. Will be called after the store added data.</p>
   * <p>Heads up: look on the protected attributes of this class to know which
   * data were used for the store add method. These will be used for the
   * {@link Store#add(String, ChunkMeta, String, IndexMeta, Handler)} method.
   * Use context.assert ... and context.fail to validate the test.</p>
   * @param context The current test context.
   * @param vertx A Vert.x instance of one test.
   * @param path The path where the data was created (may be null if not used
   * for {@link #prepareData(TestContext, Vertx, String, Handler)})
   * @param handler will be called when the validation has finished
   */
  protected abstract void validateAfterStoreAdd(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler);

  /**
   * <p>Validate the delete method of a test. Will be called after the store
   * deleted data.</p>
   * <p>Heads up: look on the protected attributes and your
   * {@link #prepareData(TestContext, Vertx, String, Handler)} implementation
   * to know which data you have deleted with the
   * {@link Store#delete(String, String, Handler)} method.
   * Use context.assert ... and context.fail to validate the test.</p>
   * @param context The current test context.
   * @param vertx A Vert.x instance of one test.
   * @param path The path where the data were created (may be null if not used
   * for {@link #prepareData(TestContext, Vertx, String, Handler)})
   * @param handler will be called when the validation has finished
   */
  protected abstract void validateAfterStoreDelete(TestContext context,
      Vertx vertx, String path, Handler<AsyncResult<Void>> handler);

  private void mockIndexerQuery(Vertx vertx, TestContext context, Async async, String path) {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(request -> {
      JsonObject msg = request.body();

      context.assertTrue(msg.containsKey("size"));
      context.assertTrue(msg.containsKey("search"));

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
    testAdd(context, null);
  }

  /**
   * Call {@link #testAdd(TestContext, String)} with a path.
   * @param context Test context
   */
  @Test
  public void testAddWithSubfolder(TestContext context) {
    testAdd(context, TEST_FOLDER);
  }

  /**
   * Call {@link #testDelete(TestContext, String)} with null as path.
   * @param context Test context
   */
  @Test
  public void testDeleteWithoutSubfolder(TestContext context) {
    testDelete(context, null);
  }

  /**
   * Call {@link #testDelete(TestContext, String)} with a path.
   * @param context Test context
   */
  @Test
  public void testDeleteWithSubfolder(TestContext context) {
    testDelete(context, TEST_FOLDER);
  }

  /**
   * Call {@link #testGet(TestContext, String)} with null as path.
   * @param context Test context
   */
  @Test
  public void testGetWithoutSubfolder(TestContext context) {
    testGet(context, null);
  }

  /**
   * Call {@link #testGet(TestContext, String)} with a path.
   * @param context Test context
   */
  @Test
  public void testGetWithSubfolder(TestContext context) {
    testGet(context, TEST_FOLDER);
  }

  /**
   * Call {@link #testGetOne(TestContext, String)} with null as path.
   * @param context Test context
   */
  @Test
  public void testGetOneWithoutFolder(TestContext context) {
    testGetOne(context, null);
  }

  /**
   * Apply the {@link Store#delete(String, String, Handler)} with a
   * non-existing path and expects a success (no exceptions or failure codes).
   * @param context Test context
   */
  @Test
  public void testDeleteNonExistingEntity(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    Async asyncIndexerQuery = context.async();

    Store store = createStore(vertx);

    // register add
    vertx.eventBus().consumer(AddressConstants.INDEXER_ADD).handler(h ->
      context.fail("Indexer should not be notified for a add event after "
          + "Store::delete was called!"));

    // register delete
    vertx.eventBus().consumer(AddressConstants.INDEXER_DELETE).handler(r ->
      context.fail("INDEXER_DELETE should not be notified if no file was found."));

    // register query
    vertx.eventBus().consumer(AddressConstants.INDEXER_QUERY).handler(r -> {
      r.fail(404, "NOT FOUND");
      asyncIndexerQuery.complete();
    });

    store.delete(SEARCH, "NOT_EXISTING_PATH", context.asyncAssertSuccess(h -> {
      async.complete();
    }));
  }

  /**
   * Apply the {@link Store#delete(String, String, Handler)} with a
   * existing path but non existing entity and expects a success (no exceptions or failure codes).
   * @param context Test context
   */
  @Test
  public void testDeleteNonExistingEntityWithPath(TestContext context) {
    Vertx vertx = rule.vertx();
    Async asyncIndexerQuery = context.async();
    Async asyncIndexerDelete = context.async();
    Async asyncDelete = context.async();

    Store store = createStore(vertx);

    // register add
    vertx.eventBus().consumer(AddressConstants.INDEXER_ADD).handler(h ->
        context.fail("Indexer should not be notified for a add event after"
            + "Store::delete was called!"));

    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(req -> {
      JsonObject msg = req.body();
      context.assertTrue(msg.containsKey("paths"));

      JsonArray paths = msg.getJsonArray("paths");
      context.assertEquals(1, paths.size());

      String notifiedPath = paths.getString(0);
      context.assertEquals(PATH_TO_NON_EXISTING_ENTITY, notifiedPath);

      req.reply(null); // Value is not used in Store

      asyncIndexerDelete.complete();
    });

    // register query
    // the null argument depend on the static PATH_TO_NON_EXISTING_ENTITY attribute value
    mockIndexerQuery(vertx, context, asyncIndexerQuery, null);

    store.delete(SEARCH, null, context.asyncAssertSuccess(h -> {
      asyncDelete.complete();
    }));
  }

  /**
   * <p>Add test data to a storage and retrieve the data with the
   * {@link Store#getOne(String, Handler)} method to compare them.</p>
   * <p>Uses {@link #prepareData(TestContext, Vertx, String, Handler)}</p>
   * @param context Test context
   * @param path The path where to look for data (may be null)
   */
  public void testGetOne(TestContext context, String path) {
    Vertx vertx = rule.vertx();
    Async async = context.async();

    prepareData(context, vertx, path, context.asyncAssertSuccess(resultPath -> {
      Store store = createStore(vertx);
      store.getOne(ID, context.asyncAssertSuccess(h -> {
        h.handler(buffer -> {
          String receivedChunk = new String(buffer.getBytes());
          context.assertEquals(CHUNK_CONTENT, receivedChunk);
        }).endHandler(end -> async.complete());
      }));
    }));
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

    Store store = createStore(vertx);

    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> {
      JsonObject index = h.body();

      context.assertEquals(META.toJsonObject(), index.getJsonObject("meta"));
      context.assertEquals(new JsonArray(TAGS), index.getJsonArray("tags"));
      context.assertEquals(new JsonObject(PROPERTIES), index.getJsonObject("properties"));
      context.assertEquals(FALLBACK_CRS_STRING, index.getString("fallbackCRSString"));

      asyncIndexerAdd.complete();
    });

    // register delete
    vertx.eventBus().consumer(AddressConstants.INDEXER_DELETE).handler(h ->
      context.fail("Indexer should not be notified for a delete event after"
          + "Store::add was called!"));

    // register query
    vertx.eventBus().consumer(AddressConstants.INDEXER_QUERY).handler(h ->
      context.fail("Indexer should not be notified for a query event after"
          + "Store::add was called!"));

    IndexMeta indexMeta = new IndexMeta(IMPORT_ID, ID, TIMESTAMP, TAGS, PROPERTIES, FALLBACK_CRS_STRING);
    store.add(CHUNK_CONTENT, META, path, indexMeta, context.asyncAssertSuccess(err -> {
      validateAfterStoreAdd(context, vertx, path, context.asyncAssertSuccess(v -> {
        asyncAdd.complete();
      }));
    }));
  }

  /**
   * Add test data and try to delete them with the
   * {@link Store#delete(String, String, Handler)} method, then check the
   * storage for any data
   * @param context Test context
   * @param path Path where the data can be found (may be null)
   */
  public void testDelete(TestContext context, String path) {
    Vertx vertx = rule.vertx();
    Async asyncIndexerQuery = context.async();
    Async asyncIndexerDelete = context.async();
    Async asyncDelete = context.async();

    prepareData(context, vertx, path, context.asyncAssertSuccess(resultPath -> {
      Store store = createStore(vertx);

      // register add
      vertx.eventBus().consumer(AddressConstants.INDEXER_ADD).handler(h ->
        context.fail("Indexer should not be notified for a add event after"
            + "Store::delete was called!"));

      // register delete
      vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(req -> {
        JsonObject msg = req.body();
        context.assertTrue(msg.containsKey("paths"));

        JsonArray paths = msg.getJsonArray("paths");
        context.assertEquals(1, paths.size());

        String notifiedPath = paths.getString(0);
        context.assertEquals(resultPath, notifiedPath);

        req.reply(null); // Value is not used in Store

        asyncIndexerDelete.complete();
      });

      // register query
      mockIndexerQuery(vertx, context, asyncIndexerQuery, path);

      store.delete(SEARCH, path, context.asyncAssertSuccess(h -> {
        validateAfterStoreDelete(context, vertx, resultPath, context.asyncAssertSuccess(v -> {
          asyncDelete.complete();
        }));
      }));
    }));
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
    mockIndexerQuery(vertx, context, asyncQuery, path);

    // register delete
    vertx.eventBus().consumer(AddressConstants.INDEXER_DELETE).handler(h ->
      context.fail("Indexer should not be notified for a delete event after"
          + "Store::get was called!"));

    // register query
    vertx.eventBus().consumer(AddressConstants.INDEXER_ADD).handler(h ->
      context.fail("Indexer should not be notified for an add event after"
          + "Store::get was called!"));

    prepareData(context, vertx, path, context.asyncAssertSuccess(resultPath -> {
      Store store = createStore(vertx);

      store.get(SEARCH, resultPath, ar -> {
        StoreCursor cursor = ar.result();
        context.assertTrue(cursor.hasNext());

        cursor.next(h -> {
          ChunkMeta meta = h.result();

          context.assertEquals(END, meta.getEnd());
          context.assertEquals(START, meta.getStart());

          String fileName = cursor.getChunkPath();

          context.assertEquals(PathUtils.join(path, ID), fileName);

          asyncGet.complete();
        });
      });
    }));
  }
}
