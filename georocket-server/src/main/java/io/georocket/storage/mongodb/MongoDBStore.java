package io.georocket.storage.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Queue;

import org.bson.types.ObjectId;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Stores chunks in MongoDB
 * @author Michel Kraemer
 */
public class MongoDBStore extends IndexedStore {
  private final Vertx vertx;
  private final MongoClient mongoClient;
  private final DB database;
  private final GridFS gridfs;

  /**
   * Constructs a new store
   * @param vertx the Vert.x instance
   */
  public MongoDBStore(Vertx vertx) {
    super(vertx);
    this.vertx = vertx;
    
    JsonObject config = vertx.getOrCreateContext().config();
    String host = config.getString(ConfigConstants.STORAGE_MONGODB_HOST);
    int port = config.getInteger(ConfigConstants.STORAGE_MONGODB_PORT, 27017);
    String databaseName = config.getString(ConfigConstants.STORAGE_MONGODB_DATABASE);
    
    mongoClient = new MongoClient(host, port);
    // TODO getDB is deprecated. Use new GridFS API as soon as it's available
    database = mongoClient.getDB(databaseName);
    gridfs = new GridFS(database);
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    vertx.<GridFSDBFile>executeBlocking(f -> {
      GridFSDBFile file = gridfs.findOne(PathUtils.normalize(path));
      f.complete(file);
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        GridFSDBFile file = ar.result();
        long size = file.getLength();
        InputStream is = file.getInputStream();
        handler.handle(Future.succeededFuture(new MongoDBChunkReadStream(is, size, vertx)));
      }
    });
  }

  @Override
  protected void doAddChunk(String chunk, String path, Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    
    // generate new file name
    String id = new ObjectId().toString();
    String filename = PathUtils.join(path, id);
    
    vertx.executeBlocking(f -> {
      GridFSInputFile file = gridfs.createFile(filename);
      try (OutputStream os = file.getOutputStream();
          OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
        writer.write(chunk);
      } catch (IOException e) {
        f.fail(e);
        return;
      }
      f.complete(filename);
    }, handler);
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }
    
    String path = PathUtils.normalize(paths.poll());
    vertx.executeBlocking(f -> {
      gridfs.remove(path);
      f.complete();
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        doDeleteChunks(paths, handler);
      }
    });
  }
}
