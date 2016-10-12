package io.georocket.storage.mongodb;

import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import io.georocket.util.StoreSummaryBuilder;
import org.bson.Document;

import com.google.common.base.Preconditions;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.async.client.gridfs.AsyncInputStream;
import com.mongodb.async.client.gridfs.GridFSBucket;
import com.mongodb.async.client.gridfs.GridFSBuckets;
import com.mongodb.async.client.gridfs.GridFSDownloadStream;
import com.mongodb.async.client.gridfs.GridFSFindIterable;
import com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Stores chunks in MongoDB
 * @author Michel Kraemer
 */
public class MongoDBStore extends IndexedStore {
  private final Context context;
  private final String connectionString;
  private final String databaseName;

  private MongoClient mongoClient;
  private MongoDatabase database;
  private GridFSBucket gridfs;

  /**
   * Constructs a new store
   * @param vertx the Vert.x instance
   */
  public MongoDBStore(Vertx vertx) {
    super(vertx);
    context = vertx.getOrCreateContext();

    JsonObject config = context.config();

    connectionString = config.getString(ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING);
    Preconditions.checkNotNull(connectionString, "Missing configuration item \"" +
        ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING + "\"");

    databaseName = config.getString(ConfigConstants.STORAGE_MONGODB_DATABASE);
    Preconditions.checkNotNull(connectionString, "Missing configuration item \"" +
        ConfigConstants.STORAGE_MONGODB_DATABASE + "\"");
  }

  /**
   * Get or create the MongoDB client
   * @return the MongoDB client
   */
  private MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient = MongoClients.create(connectionString);
    }
    return mongoClient;
  }

  /**
   * Get or create the MongoDB database
   * @return the MongoDB client
   */
  private MongoDatabase getDB() {
    if (database == null) {
      database = getMongoClient().getDatabase(databaseName);
    }
    return database;
  }

  /**
   * Get or create the MongoDB GridFS instance
   * @return the MongoDB client
   */
  private GridFSBucket getGridFS() {
    if (gridfs == null) {
      gridfs = GridFSBuckets.create(getDB());
    }
    return gridfs;
  }
  
  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    GridFSDownloadStream downloadStream =
        getGridFS().openDownloadStream(PathUtils.normalize(path));
    downloadStream.getGridFSFile((file, t) -> {
      context.runOnContext(v -> {
        if (t != null) {
          handler.handle(Future.failedFuture(t));
        } else {
          long length = file.getLength();
          int chunkSize = file.getChunkSize();
          handler.handle(Future.succeededFuture(new MongoDBChunkReadStream(
              downloadStream, length, chunkSize, context)));
        }
      });
    });
  }

  @Override
  public void getSize(Handler<AsyncResult<Long>> handler) {
    AtomicLong total = new AtomicLong();
    getGridFS().find().forEach(file -> {
      total.getAndAdd(file.getLength());
    }, (v, t) -> {
      context.runOnContext(v2 -> {
        if (t != null) {
          handler.handle(Future.failedFuture(t));
        } else {
          handler.handle(Future.succeededFuture(total.get()));
        }
      });
    });
  }

  @Override
  public void getStoreSummary(Handler<AsyncResult<JsonObject>> handler) {
    StoreSummaryBuilder summaryBuilder = new StoreSummaryBuilder();

    getGridFS().find().forEach(file -> {
      summaryBuilder.put(extractLayer(file.getFilename()),
          file.getLength(), file.getUploadDate().getTime());
    }, (v, t) -> {
      context.runOnContext(v2 -> {
        if (t != null) {
          handler.handle(Future.failedFuture(t));
        } else {
          handler.handle(Future.succeededFuture(summaryBuilder.finishBuilding()));
        }
      });
    });
  }

  private static String extractLayer(String name) {
    // I expect exactly two types of names
    // 1) /layer/id
    // 2) /id
    // One with two slashes and one only with one.
    String parts[] = name.split("/");

    // TODO: currently the names are prefixed with "/store"
    // TODO: pull from georocket upstream master and decrement all numbers by one
    String layer = parts.length == 3
      ? "/" : parts.length == 4 ? "/" + parts[2] : null;

    return layer;
  }

  @Override
  protected void doAddChunk(String chunk, String path, Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }

    // generate new file name
    String id = generateChunkId();
    String filename = PathUtils.join(path, id);
    
    byte[] bytes = chunk.getBytes(StandardCharsets.UTF_8);
    AsyncInputStream is = AsyncStreamHelper.toAsyncInputStream(bytes);
    getGridFS().uploadFromStream(filename, is, (oid, t) -> {
      context.runOnContext(v -> {
        if (t != null) {
          handler.handle(Future.failedFuture(t));
        } else {
          handler.handle(Future.succeededFuture(filename));
        }
      });
    });
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }

    String path = PathUtils.normalize(paths.poll());
    GridFSBucket gridFS = getGridFS();
    GridFSFindIterable i = gridFS.find(new Document("filename", path));
    i.first((file, t) -> {
      if (t != null) {
        context.runOnContext(v -> handler.handle(Future.failedFuture(t)));
      } else {
        if (file == null) {
          // file does not exist
          context.runOnContext(v -> doDeleteChunks(paths, handler));
          return;
        }
        gridFS.delete(file.getObjectId(), (r, t2) -> {
          context.runOnContext(v -> {
            if (t2 != null) {
              handler.handle(Future.failedFuture(t2));
            } else {
              doDeleteChunks(paths, handler);
            }
          });
        });
      }
    });
  }
}
