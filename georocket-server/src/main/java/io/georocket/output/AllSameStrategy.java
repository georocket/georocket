package io.georocket.output;

import io.georocket.storage.ChunkMeta;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Merge chunks whose root XML elements are all equal
 * @author Michel Kraemer
 */
public class AllSameStrategy extends AbstractXMLMergeStrategy {
  @Override
  public void canMerge(ChunkMeta meta, Handler<AsyncResult<Boolean>> handler) {
    if (getParents() == null || getParents().equals(meta.getParents())) {
      handler.handle(ASYNC_TRUE);
    } else {
      handler.handle(ASYNC_FALSE);
    }
  }

  @Override
  protected void mergeParents(ChunkMeta meta, Handler<AsyncResult<Void>> handler) {
    if (getParents() == null) {
      setParents(meta.getParents());
    }
    handler.handle(Future.succeededFuture());
  }
}
