package io.georocket.index;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

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
      + "gml:id=\"" + TESTFILE_GMLID1 + "\"></test>";
  private static final String TESTFILE_NAME1 = "abcd";
  private static final ChunkMeta TESTFILE_META1 = new ChunkMeta(
      Collections.emptyList(), 0, TESTFILE_CONTENTS1.length());

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
  
  @Before
  public void setUp() {
    storagePath = new File(folder.getRoot(), "storage");
    JsonObject config = new JsonObject()
        .put(ConfigConstants.STORAGE_PATH, storagePath.getAbsolutePath());
    options = new DeploymentOptions().setConfig(config);
  }
  
  /**
   * Write a simple test file to the storage
   * @param context the test context
   */
  private void writeTestFile1(TestContext context) {
    File filePath = new File(storagePath, "file");
    File f = new File(filePath, TESTFILE_NAME1);
    try {
      FileUtils.writeStringToFile(f, TESTFILE_CONTENTS1, StandardCharsets.UTF_8);
    } catch (IOException e) {
      context.fail(e);
    }
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
        vertx.undeploy(ivid, context.asyncAssertSuccess(v -> {
          async.complete();
        }));
      }));
    }));
  }
  
  private void deployAndTestFile1(TestContext context,
      Handler<AsyncResult<String>> handler) {
    Vertx vertx = rule.vertx();
    
    // deploy indexer verticle
    vertx.deployVerticle(IndexerVerticle.class.getName(), options,
        context.asyncAssertSuccess(ivid -> {
      // save a test file in the storage directory
      writeTestFile1(context);
      
      // tell indexer verticle about the new file
      JsonObject msg = new JsonObject()
          .put("action", "add")
          .put("path", TESTFILE_NAME1)
          .put("meta", TESTFILE_META1.toJsonObject());
      vertx.eventBus().send(AddressConstants.INDEXER, msg, ar -> {
        if (ar.failed()) {
          handler.handle(Future.failedFuture(ar.cause().getMessage()));
        } else {
          handler.handle(Future.succeededFuture(ivid));
        }
      });
    }));
  }
  
  /**
   * Test if a chunk can be indexed successfully
   * @param context the test context
   */
  @Test
  public void add(TestContext context) {
    Async async = context.async();
    deployAndTestFile1(context, context.asyncAssertSuccess(id -> {
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
    deployAndTestFile1(context, context.asyncAssertSuccess(id -> {
      // wait for Elasticsearch to update the index
      vertx.setTimer(2000, l -> {
        JsonObject msg = new JsonObject()
            .put("action", "query")
            .put("search", TESTFILE_GMLID1);
        vertx.eventBus().<JsonObject>send(AddressConstants.INDEXER, msg, context.asyncAssertSuccess(reply -> {
          JsonObject obj = reply.body();
          int totalHits = obj.getInteger("totalHits");
          context.assertEquals(1, totalHits);
          rule.vertx().undeploy(id, context.asyncAssertSuccess(v -> {
            async.complete();
          }));
        }));
      });
    }));
  }
}
