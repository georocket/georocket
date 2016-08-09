package io.georocket.storage.mongodb;

import java.io.IOException;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.mongodb.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.connection.ClusterSettings;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link MongoDBChunkReadStream}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class MongoDBChunkReadStreamTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  private static MongoDBTestConnector mongoConnector;
  
  /**
   * Set up test dependencies
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
   * Create a file in GridFS with the given filename and write
   * some random data to it.
   * @param filename the name of the file to create
   * @param size the number of random bytes to write
   * @param vertx the Vert.x instance
   * @param handler a handler that will be called when the file
   * has been written
   */
  private void prepareData(String filename, int size, Vertx vertx,
      Handler<AsyncResult<String>> handler) {
    vertx.<String>executeBlocking(f -> {
      try (MongoClient client = new MongoClient(mongoConnector.serverAddress)) {
        MongoDatabase db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME);
        GridFSBucket gridFS = GridFSBuckets.create(db);
        try (GridFSUploadStream os = gridFS.openUploadStream(filename)) {
          for (int i = 0; i < size; ++i) {
            os.write((byte)(i & 0xFF));
          }
        }
      }
      f.complete(filename);
    }, handler);
  }
  
  /**
   * Connect to MongoDB and get the GridFS chunk size
   * @param vertx the Vert.x instance
   * @param handler a handler that will be called with the chunk size
   */
  private void getChunkSize(Vertx vertx, Handler<AsyncResult<Integer>> handler) {
    vertx.<Integer>executeBlocking(f -> {
      try (MongoClient client = new MongoClient(mongoConnector.serverAddress)) {
        MongoDatabase db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME);
        GridFSBucket gridFS = GridFSBuckets.create(db);
        f.complete(gridFS.getChunkSizeBytes());
      }
    }, handler);
  }
  
  /**
   * Create an asynchronous MongoDB client
   * @return the client
   */
  private com.mongodb.async.client.MongoClient createAsyncClient() {
    ClusterSettings clusterSettings = ClusterSettings.builder()
        .hosts(Arrays.asList(mongoConnector.serverAddress))
        .build();
    MongoClientSettings settings = MongoClientSettings.builder()
        .clusterSettings(clusterSettings).build();
    return MongoClients.create(settings);
  }
  
  /**
   * Test if a tiny file can be read
   * @param context the test context
   */
  @Test
  public void tiny(TestContext context) {
    Vertx vertx = rule.vertx();
    getChunkSize(vertx, context.asyncAssertSuccess(cs -> {
      doRead(2, cs, vertx, context);
    }));
  }
  
  /**
   * Test if a small file can be read
   * @param context the test context
   */
  @Test
  public void small(TestContext context) {
    Vertx vertx = rule.vertx();
    getChunkSize(vertx, context.asyncAssertSuccess(cs -> {
      doRead(1024, cs, vertx, context);
    }));
  }
  
  /**
   * Test if a file can be read whose size equals the
   * default GridFS chunk size
   * @param context the test context
   */
  @Test
  public void defaultChunkSize(TestContext context) {
    Vertx vertx = rule.vertx();
    getChunkSize(vertx, context.asyncAssertSuccess(cs -> {
      doRead(cs, cs, vertx, context);
    }));
  }
  
  /**
   * Test if a medium-sized file can be read
   * @param context the test context
   */
  @Test
  public void medium(TestContext context) {
    Vertx vertx = rule.vertx();
    getChunkSize(vertx, context.asyncAssertSuccess(cs -> {
      doRead(cs * 3 / 2, cs, vertx, context);
    }));
  }
  
  /**
   * Test if a large file can be read
   * @param context the test context
   */
  @Test
  public void large(TestContext context) {
    Vertx vertx = rule.vertx();
    getChunkSize(vertx, context.asyncAssertSuccess(cs -> {
      doRead(cs * 10 + 100, cs, vertx, context);
    }));
  }
  
  /**
   * The actual test method. Creates a temporary file with random contents. Writes
   * <code>size</code> bytes to it and reads it again through
   * {@link MongoDBChunkReadStream}. Finally, checks if the file has been read correctly.
   * @param size the number of bytes to write/read
   * @param chunkSize the GridFS chunk size
   * @param vertx the Vert.x instance
   * @param context the current test context
   */
  private void doRead(int size, int chunkSize, Vertx vertx, TestContext context) {
    Async async = context.async();
    
    // create a test file in GridFS
    prepareData("test_" + size + ".bin", size, vertx, context.asyncAssertSuccess(filename -> {
      // connect to GridFS
      com.mongodb.async.client.MongoClient client = createAsyncClient();
      com.mongodb.async.client.MongoDatabase db =
          client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME);
      com.mongodb.async.client.gridfs.GridFSBucket gridfs =
          com.mongodb.async.client.gridfs.GridFSBuckets.create(db);
      
      // open the test file
      GridFSDownloadStream is = gridfs.openDownloadStream(filename);
      MongoDBChunkReadStream rs = new MongoDBChunkReadStream(is, size, chunkSize,
          vertx.getOrCreateContext());
      
      // read from the test file
      rs.exceptionHandler(context::fail);
      
      int[] pos = { 0 };
      
      rs.endHandler(v -> {
        // the file has been completely read
        rs.close();
        context.assertEquals(size, pos[0]);
        async.complete();
      });
      
      rs.handler(buf -> {
        // check number of read bytes
        if (size - pos[0] > chunkSize) {
          context.assertEquals(chunkSize, buf.length());
        } else {
          context.assertEquals(size - pos[0], buf.length());
        }
        
        // check file contents
        for (int i = pos[0]; i < pos[0] + buf.length(); ++i) {
          context.assertEquals((byte)(i & 0xFF), buf.getByte(i - pos[0]));
        }
        
        pos[0] += buf.length();
      });
    }));
  }
}
