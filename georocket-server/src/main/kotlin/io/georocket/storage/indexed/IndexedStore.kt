package io.georocket.storage.indexed

import io.georocket.storage.Store
import io.georocket.storage.StoreCursor
import io.vertx.core.Vertx

/**
 * An abstract base class for chunk stores that are backed by an indexer
 * @author Michel Kraemer
 */
abstract class IndexedStore(private val vertx: Vertx) : Store {
  override suspend fun get(search: String?, path: String): StoreCursor {
    return IndexedStoreCursor(vertx, search, path).start()
  }

  override suspend fun scroll(search: String?, path: String, size: Int): StoreCursor {
    return FrameCursor(vertx, search, path, size).start()
  }

  override suspend fun scroll(scrollId: String): StoreCursor {
    return FrameCursor(vertx, scrollId = scrollId).start()
  }
}
