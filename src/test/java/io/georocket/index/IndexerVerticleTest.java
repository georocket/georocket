package io.georocket.index;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkMeta;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link IndexerVerticle}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class IndexerVerticleTest {
  private static final String TESTFILE_GMLID1 = "1234";
  private static final String TESTFILE_CONTENTS1 = "<test "
      + "xmlns:gml=\"http://www.opengis.net/gml\" "
      + "gml:id=\"" + TESTFILE_GMLID1 + "\">"
      + "</test>";
  private static final String TESTFILE_NAME1 = "abcd";
  private static final ChunkMeta TESTFILE_META1 = new ChunkMeta(
      Collections.emptyList(), 0, TESTFILE_CONTENTS1.length());
  
  private static final String TESTFILE_GMLID2 = "5678";
  private static final String TESTFILE_CONTENTS2 = "<test "
      + "xmlns:gml=\"http://www.opengis.net/gml\" "
      + "gml:id=\"" + TESTFILE_GMLID2 + "\">"
      + "<posList srsName=\"epsg:4326\">13.047058991922242 52.33014282539029 "
      + "13.047215442741708 52.33021464459186</posList>"
      + "</test>";
  private static final String TESTFILE_NAME2 = "efgh";
  private static final ChunkMeta TESTFILE_META2 = new ChunkMeta(
      Collections.emptyList(), 0, TESTFILE_CONTENTS2.length());

  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  /**
   * Create a temporary folder
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  /**
   * The path to the IndexerVerticle's storage
   */
  private File storagePath;
  
  /**
   * Deployment options for the verticle
   */
  private DeploymentOptions options;
  
  /**
   * Set up test environment
   */
  @Before
  public void setUp() {
    storagePath = new File(folder.getRoot(), "storage");
    JsonObject config = new JsonObject()
        .put(ConfigConstants.STORAGE_PATH, storagePath.getAbsolutePath());
    options = new DeploymentOptions().setConfig(config);
  }
  
  /**
   * Write a test file to the storage
   * @param context the test context
   * @param name the name of the test file
   * @param contents the file contents
   */
  private void writeTestFile(TestContext context, String name, String contents) {
    File filePath = new File(storagePath, "file");
    File f = new File(filePath, name);
    try {
      FileUtils.writeStringToFile(f, contents, StandardCharsets.UTF_8);
    } catch (IOException e) {
      context.fail(e);
    }
  }
  
  /**
   * Write test file 1 to the storage
   * @param context the test context
   */
  private void writeTestFile1(TestContext context) {
    writeTestFile(context, TESTFILE_NAME1, TESTFILE_CONTENTS1);
  }
  
  /**
   * Write test file 2 to the storage
   * @param context the test context
   */
  private void writeTestFile2(TestContext context) {
    writeTestFile(context, TESTFILE_NAME2, TESTFILE_CONTENTS2);
  }
  
  /**
   * Test if the verticle fails if there is no path given
   * @param context the test context
   */
  @Test
  public void addNoPath(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    
    // deploy indexer verticle
    vertx.deployVerticle(IndexerVerticle.class.getName(), options,
        context.asyncAssertSuccess(ivid -> {
      JsonObject msg = new JsonObject()
          .put("action", "add");
      vertx.eventBus().send(AddressConstants.INDEXER, msg, context.asyncAssertFailure(t -> {
        context.assertEquals(400, ((ReplyException)t).failureCode());
        context.assertEquals("Missing path to the chunk to index", t.getMessage());
        vertx.undeploy(ivid, context.asyncAssertSuccess(v -> {
          async.complete();
        }));
      }));
    }));
  }
  
  /**
   * Test if the verticle fails if there is no metadata given
   * @param context the test context
   */
  @Test
  public void addNoMetadata(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    
    // deploy indexer verticle
    vertx.deployVerticle(IndexerVerticle.class.getName(), options,
        context.asyncAssertSuccess(ivid -> {
      JsonObject msg = new JsonObject()
          .put("action", "add")
          .put("path", TESTFILE_NAME1);
      vertx.eventBus().send(AddressConstants.INDEXER, msg, context.asyncAssertFailure(t -> {
        context.assertEquals(400, ((ReplyException)t).failureCode());
        context.assertEquals("Missing chunk metadata", t.getMessage());
        vertx.undeploy(ivid, context.asyncAssertSuccess(v -> {
          async.complete();
        }));
      }));
    }));
  }
  
  /**
   * Test if the verticle fails if the chunk was not found
   * @param context the test context
   */
  @Test
  public void addNoChunk(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    
    // deploy indexer verticle
    vertx.deployVerticle(IndexerVerticle.class.getName(), options,
        context.asyncAssertSuccess(ivid -> {
      JsonObject msg = new JsonObject()
          .put("action", "add")
          .put("path", TESTFILE_NAME1)
          .put("meta", TESTFILE_META1.toJsonObject());
      vertx.eventBus().send(AddressConstants.INDEXER, msg, context.asyncAssertFailure(t -> {
        context.assertEquals(404, ((ReplyException)t).failureCode());
        context.assertEquals("Chunk not found: " + TESTFILE_NAME1, t.getMessage());
        vertx.undeploy(ivid, context.asyncAssertSuccess(v -> {
          async.complete();
        }));
      }));
    }));
  }
  
  private void deployAndTestFiles(TestContext context, List<String> tags1,
      Handler<AsyncResult<String>> handler) {
    Vertx vertx = rule.vertx();
    
    // deploy indexer verticle
    vertx.deployVerticle(IndexerVerticle.class.getName(), options,
        context.asyncAssertSuccess(ivid -> {
      // save test files in the storage directory
      writeTestFile1(context);
      writeTestFile2(context);
      
      // tell indexer verticle about the new files
      JsonObject msg1 = new JsonObject()
          .put("action", "add")
          .put("path", TESTFILE_NAME1)
          .put("meta", TESTFILE_META1.toJsonObject());
      if (tags1 != null) {
        msg1.put("tags", new JsonArray(tags1));
      }
      vertx.eventBus().send(AddressConstants.INDEXER, msg1, context.asyncAssertSuccess(ar1 -> {
        JsonObject msg2 = new JsonObject()
            .put("action", "add")
            .put("path", TESTFILE_NAME2)
            .put("meta", TESTFILE_META2.toJsonObject());
        vertx.eventBus().send(AddressConstants.INDEXER, msg2, context.asyncAssertSuccess(ar2 -> {
          // wait for Elasticsearch to update the index
          vertx.setTimer(2000, l -> {
            handler.handle(Future.succeededFuture(ivid));
          });
        }));
      }));
    }));
  }
  
  private void deployAndTestFiles(TestContext context,
      Handler<AsyncResult<String>> handler) {
    deployAndTestFiles(context, null, handler);
  }
  
  /**
   * Test if a chunk can be indexed successfully
   * @param context the test context
   */
  @Test
  public void add(TestContext context) {
    Async async = context.async();
    deployAndTestFiles(context, context.asyncAssertSuccess(id -> {
      rule.vertx().undeploy(id, context.asyncAssertSuccess(v -> {
        async.complete();
      }));
    }));
  }
  
  /**
   * Test if a we can query for a GML id
   * @param context the test context
   */
  @Test
  public void queryGmlId(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    deployAndTestFiles(context, context.asyncAssertSuccess(id -> {
      // search for test file 1
      JsonObject msg1 = new JsonObject()
          .put("action", "query")
          .put("search", TESTFILE_GMLID1);
      vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, msg1, context.asyncAssertSuccess(reply1 -> {
        JsonObject obj1 = reply1.body();
        int totalHits1 = obj1.getInteger("totalHits");
        context.assertEquals(1, totalHits1);
        
        // perform a query that should return no result
        JsonObject msg2 = new JsonObject()
            .put("action", "query")
            .put("search", "zzzz");
        vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, msg2, context.asyncAssertSuccess(reply2 -> {
          JsonObject obj2 = reply2.body();
          int totalHits2 = obj2.getInteger("totalHits");
          context.assertEquals(0, totalHits2);
        
          // undeploy verticle
          rule.vertx().undeploy(id, context.asyncAssertSuccess(v -> {
            async.complete();
          }));
        }));
      }));
    }));
  }
  
  /**
   * Test if a we can perform a spatial query
   * @param context the test context
   */
  @Test
  public void spatialQuery(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    deployAndTestFiles(context, context.asyncAssertSuccess(id -> {
      // search for test file 2
      JsonObject msg1 = new JsonObject()
          .put("action", "query")
          .put("search", "13.0,52.2,13.1,52.4");
      vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, msg1, context.asyncAssertSuccess(reply1 -> {
        JsonObject obj1 = reply1.body();
        int totalHits1 = obj1.getInteger("totalHits");
        context.assertEquals(1, totalHits1);
        
        // perform a search that should return no result
        JsonObject msg2 = new JsonObject()
            .put("action", "query")
            .put("search", "12.0,51.2,12.1,51.4");
        vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, msg2, context.asyncAssertSuccess(reply2 -> {
          JsonObject obj2 = reply2.body();
          int totalHits2 = obj2.getInteger("totalHits");
          context.assertEquals(0, totalHits2);
          
          // undeploy verticle
          rule.vertx().undeploy(id, context.asyncAssertSuccess(v -> {
            async.complete();
          }));
        }));
      }));
    }));
  }
  
  /**
   * Test if a we can query for tags
   * @param context the test context
   */
  @Test
  public void queryTags(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();
    deployAndTestFiles(context, Arrays.asList("tag1a", "tag1b"), context.asyncAssertSuccess(id -> {
      // search for test file 2
      JsonObject msg1 = new JsonObject()
          .put("action", "query")
          .put("search", "tag1a");
      vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, msg1, context.asyncAssertSuccess(reply1 -> {
        JsonObject obj1 = reply1.body();
        int totalHits1 = obj1.getInteger("totalHits");
        context.assertEquals(1, totalHits1);
        
        // undeploy verticle
        rule.vertx().undeploy(id, context.asyncAssertSuccess(v -> {
          async.complete();
        }));
      }));
    }));
  }
}
