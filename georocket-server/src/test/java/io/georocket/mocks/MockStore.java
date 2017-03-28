package io.georocket.mocks;

import java.util.Queue;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.IndexMeta;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

public class MockStore extends IndexedStore {
  Vertx vertx;
  
  public static String RETURNED_CHUNK = "{\"type\":\"Polygon\"}"; 

  public MockStore(Vertx vertx) {
    super(vertx);
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    Buffer chunk = Buffer.buffer(RETURNED_CHUNK);
    handler.handle(Future.succeededFuture(new DelegateChunkReadStream(chunk)));
  }

  @Override
  public void delete(String search, String path, Handler<AsyncResult<Void>> handler) {
    notImplemented(handler);
  }

  @Override
  protected void doAddChunk(String chunk, String path, Handler<AsyncResult<String>> handler) {
    notImplemented(handler);
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    notImplemented(handler);
  }

  @Override
  public void add(String chunk, ChunkMeta chunkMeta, String path, IndexMeta indexMeta,
    Handler<AsyncResult<Void>> handler) {
    notImplemented(handler);
  }

  @SuppressWarnings("unchecked")
  private void notImplemented(@SuppressWarnings("rawtypes") Handler handler) {
    handler.handle(Future.failedFuture("NOT IMPLEMENTED"));
  }

}
