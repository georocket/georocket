package io.georocket.storage

import io.georocket.constants.ConfigConstants
import io.georocket.storage.file.FileStore
import io.vertx.core.Vertx

/**
 * A factory for stores
 * @author Michel Kraemer
 */
object StoreFactory {
  /**
   * Create a new store
   */
  fun createStore(vertx: Vertx): Store {
    val config = vertx.orCreateContext.config()
    val cls = config.getString(ConfigConstants.STORAGE_CLASS, FileStore::class.java.name)
    return try {
      Class.forName(cls).getConstructor(Vertx::class.java).newInstance(vertx) as Store
    } catch (e: ReflectiveOperationException) {
      throw RuntimeException("Could not create chunk store", e)
    }
  }
}
