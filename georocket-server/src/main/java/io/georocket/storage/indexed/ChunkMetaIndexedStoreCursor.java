package io.georocket.storage.indexed;

import io.georocket.storage.ChunkMeta;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Implementation of IndexdStoreCursor for Chunk Meta.
 * 
 * @author Yasmina Kammeyer
 *
 */
public class ChunkMetaIndexedStoreCursor extends IndexedStoreCursor<ChunkMeta> {

  public ChunkMetaIndexedStoreCursor(Vertx vertx, int pageSize, String search, String path) {
    super(vertx, pageSize, search, path);
  }

  @Override
  protected ChunkMeta[] createChunkMetaArray(int length) {
    return new ChunkMeta[length];
  }

  @Override
  protected ChunkMeta createChunkMetaObj(JsonObject hit) {
    return new ChunkMeta(hit);
  }

}
