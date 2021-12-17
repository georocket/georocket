package io.georocket.storage

import io.georocket.constants.ConfigConstants
import io.georocket.storage.file.FileStore
import io.georocket.storage.h2.H2Store
import io.georocket.storage.mongodb.MongoDBStore
import io.georocket.storage.s3.S3Store
import io.vertx.core.Vertx
import org.slf4j.LoggerFactory

/**
 * A factory for stores
 * @author Michel Kraemer
 */
object StoreFactory {
  private val log = LoggerFactory.getLogger(StoreFactory::class.java)

  private const val DRIVER_FILE = "file"
  private const val DRIVER_H2 = "h2"
  private const val DRIVER_MONGODB = "mongodb"
  private const val DRIVER_S3 = "s3"

  /**
   * Create a new store
   */
  suspend fun createStore(vertx: Vertx): Store {
    val config = vertx.orCreateContext.config()
    val driver = config.getString(ConfigConstants.STORAGE_DRIVER, DRIVER_MONGODB)

    log.info("Using database driver: $driver")
    return when (driver) {
      DRIVER_FILE -> FileStore(vertx)
      DRIVER_H2 -> H2Store(vertx)
      DRIVER_MONGODB -> MongoDBStore.create(vertx)
      DRIVER_S3 -> S3Store(vertx)
      else -> throw IllegalStateException("Unknown database driver `$driver'")
    }
  }
}
