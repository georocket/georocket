package io.georocket.storage

import io.georocket.constants.ConfigConstants
import io.georocket.storage.file.FileStore
import io.vertx.core.Vertx
import kotlin.reflect.full.starProjectedType

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
      val kc = Class.forName(cls).kotlin
      val constructor = kc.constructors.find { c ->
        c.parameters.isNotEmpty() && c.parameters.first().type == Vertx::class.starProjectedType
      } ?: throw RuntimeException("Storage class has no constructor with " +
          "Vert.x object as its first parameter")
      constructor.callBy(mapOf(constructor.parameters.first() to vertx)) as Store
    } catch (e: ReflectiveOperationException) {
      throw RuntimeException("Could not create chunk store", e)
    }
  }
}
