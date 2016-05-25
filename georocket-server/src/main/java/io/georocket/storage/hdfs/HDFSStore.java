package io.georocket.storage.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.bson.types.ObjectId;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.storage.mongodb.MongoDBChunkReadStream;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Stores chunks on HDFS
 * @author Michel Kraemer
 */
public class HDFSStore extends IndexedStore {
  private final Vertx vertx;
  private final Configuration configuration;
  private final String root;
  private FileSystem fs;

  /**
   * Constructs a new store
   * @param vertx the Vert.x instance
   */
  public HDFSStore(Vertx vertx) {
    super(vertx);
    this.vertx = vertx;

    JsonObject config = vertx.getOrCreateContext().config();
    root = config.getString(ConfigConstants.STORAGE_HDFS_PATH);
    String defaultFS = config.getString(ConfigConstants.STORAGE_HDFS_DEFAULT_FS);
    configuration = new Configuration();
    configuration.set("fs.defaultFS", defaultFS);
  }

  /**
   * Get or create the HDFS file system
   * Note: this method must be synchronized because we're accessing the
   * {@link #fs} field and we're calling this method from a worker thread.
   * @return the MongoDB client
   * @throws IOException if the file system instance could not be created
   */
  private synchronized FileSystem getFS() throws IOException {
    if (fs == null) {
      fs = FileSystem.get(configuration);
    }
    return fs;
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    vertx.<Pair<Long, InputStream>>executeBlocking(f -> {
      try {
        Path p = new Path(PathUtils.join(root, path));
        long size;
        FSDataInputStream is;
        synchronized (HDFSStore.this) {
          FileSystem fs = getFS();
          FileStatus status = fs.getFileStatus(p);
          size = status.getLen();
          is = fs.open(p);
        }
        f.complete(Pair.of(size, is));
      } catch (IOException e) {
        f.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        Pair<Long, InputStream> p = ar.result();
        handler.handle(Future.succeededFuture(new MongoDBChunkReadStream(p.getValue(), p.getKey(), vertx)));
      }
    });
  }

  @Override
  public void getStoredSize(Handler<AsyncResult<Long>> handler) {
    vertx.<Long>executeBlocking(f -> {
      try {
        synchronized (HDFSStore.this) {
          FileSystem fs     = getFS();
          FsStatus   status = fs.getStatus();
          f.complete(status.getUsed());
        }
      } catch (IOException e) {
        f.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handler.handle(Future.succeededFuture(ar.result()));
      }
    });
  }

  /**
   * Create a new file on HDFS
   * @param filename the file name
   * @return an output stream that you can use to write the new file
   * @throws IOException if the file cannot be created
   */
  private synchronized FSDataOutputStream createFile(String filename) throws IOException {
    return getFS().create(new Path(PathUtils.join(root, filename)), false);
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
      try {
        try (FSDataOutputStream os = createFile(filename);
            OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
          writer.write(chunk);
        }
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

    String path = PathUtils.join(root, paths.poll());
    vertx.executeBlocking(f -> {
      try {
        synchronized (HDFSStore.this) {
          FileSystem fs = getFS();
          Path hdfsPath = new Path(path);

          if (fs.exists(hdfsPath)) {
            fs.delete(hdfsPath, false);
          }
        }
      } catch (IOException e) {
        f.fail(e);
        return;
      }
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
