package io.georocket.storage.h2;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import org.h2.mvstore.MVStore;

import com.google.common.base.Preconditions;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Stores chunks on a H2 database
 * @author Michel Kraemer
 */
public class H2Store extends IndexedStore {
  /**
   * The path to the H2 database file
   */
  private final String path;

  /**
   * True if the H2 database should compress chunks using the LZF algorithm.
   * This can save a lot of disk space but will slow down read and write
   * operations slightly.
   */
  private final boolean compress;

  /**
   * The name of the MVMap within the H2 database.
   */
  private final String mapName;

  /**
   * The underlying H2 MVStore. Use {@link #getMVStore()} to retrieve this
   * field's value.
   */
  private static final AtomicReference<MVStore> mvstore = new AtomicReference<>();

  /**
   * The underlying H2 MVMap. Use {@link #getMap()} to retrieve this field's
   * value.
   */
  private Map<String, String> map;

  /**
   * Constructs a new store
   * @param vertx the Vert.x instance
   */
  public H2Store(Vertx vertx) {
    super(vertx);

    JsonObject config = vertx.getOrCreateContext().config();
    
    path = config.getString(ConfigConstants.STORAGE_H2_PATH);
    Preconditions.checkNotNull(path, "Missing configuration item \"" +
        ConfigConstants.STORAGE_H2_PATH + "\"");

    compress = config.getBoolean(ConfigConstants.STORAGE_H2_COMPRESS, false);
    mapName = config.getString(ConfigConstants.STORAGE_H2_MAP_NAME, "georocket");
  }
  
  /**
   * Release all resources and close this store
   */
  public void close() {
    MVStore s = mvstore.getAndSet(null);
    if (s != null) {
      s.close();
    }
    map = null;
  }

  /**
   * Get or create the H2 MVStore
   * @return the MVStore
   */
  protected MVStore getMVStore() {
    MVStore result = mvstore.get();
    if (result == null) {
      synchronized (mvstore) {
        MVStore.Builder builder = new MVStore.Builder()
          .fileName(path);
  
        if (compress) {
          builder = builder.compress();
        }
  
        result = builder.open();
        mvstore.set(result);
      }
    }
    return result;
  }

  /**
   * Get or create the H2 MVMap
   * @return the MVMap
   */
  protected Map<String, String> getMap() {
    if (map == null) {
      map = getMVStore().openMap(mapName);
    }
    return map;
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    String finalPath = PathUtils.normalize(path);
    String chunkStr = getMap().get(finalPath);
    if (chunkStr == null) {
      handler.handle(Future.failedFuture(new FileNotFoundException(
        "Could not find chunk: " + finalPath)));
      return;
    }

    Buffer chunk = Buffer.buffer(chunkStr);
    handler.handle(Future.succeededFuture(new DelegateChunkReadStream(chunk)));
  }

  @Override
  protected void doAddChunk(String chunk, String path, String correlationId,
      Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }

    String filename = PathUtils.join(path, generateChunkId(correlationId));
    getMap().put(filename, chunk);
    handler.handle(Future.succeededFuture(filename));
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths,
      Handler<AsyncResult<Void>> handler) {
    while (!paths.isEmpty()) {
      String path = PathUtils.normalize(paths.poll());
      getMap().remove(path);
    }
    handler.handle(Future.succeededFuture());
  }
}
