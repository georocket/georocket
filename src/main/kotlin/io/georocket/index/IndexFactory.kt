package io.georocket.index

import io.georocket.constants.ConfigConstants
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.index.postgresql.PostgreSQLIndex
import io.vertx.core.Vertx
import org.slf4j.LoggerFactory

/**
 * A factory for indexes
 * @author Michel Kraemer
 */
object IndexFactory {
  private val log = LoggerFactory.getLogger(IndexFactory::class.java)

  private const val DRIVER_FILE = "file"
  private const val DRIVER_MONGODB = "mongodb"
  private const val DRIVER_POSTGRESQL = "postgresql"

  /**
   * Create a new index
   */
  suspend fun createIndex(vertx: Vertx): Index {
    val config = vertx.orCreateContext.config()
    val driver = config.getString(ConfigConstants.INDEX_DRIVER, DRIVER_MONGODB)

    log.info("Using database driver: $driver")
    val index = when (driver) {
      DRIVER_MONGODB -> MongoDBIndex.create(vertx)
      DRIVER_POSTGRESQL -> PostgreSQLIndex.create(vertx)
      else -> throw IllegalStateException("Unknown database driver `$driver'")
    }

    val indexedFields = config.getJsonArray(ConfigConstants.INDEX_INDEXED_FIELDS)?.map { it.toString() } ?: emptyList()
    val databaseIndexes = (MetaIndexerFactory.ALL + IndexerFactory.ALL)
      .flatMap { it.getDatabaseIndexes(indexedFields) }
    index.setUpDatabaseIndexes(databaseIndexes)
    return index
  }
}
