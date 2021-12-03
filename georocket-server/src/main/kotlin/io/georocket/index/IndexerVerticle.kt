package io.georocket.index

import io.georocket.constants.AddressConstants
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.util.FilteredServiceLoader
import io.georocket.util.ThrowableHelper.throwableToCode
import io.georocket.util.ThrowableHelper.throwableToMessage
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch

/**
 * Generic methods for background indexing of any messages
 * @author Michel Kraemer
 */
class IndexerVerticle : CoroutineVerticle() {
  companion object {
    private val log = LoggerFactory.getLogger(IndexerVerticle::class.java)
  }

  /**
   * The GeoRocket index
   */
  private lateinit var index: Index

  /**
   * The GeoRocket store
   */
  private lateinit var store: Store

  /**
   * A list of [MetaIndexerFactory] objects
   */
  private lateinit var metaIndexerFactories: List<MetaIndexerFactory>

  /**
   * A list of [IndexerFactory] objects
   */
  private lateinit var indexerFactories: List<IndexerFactory>

  override suspend fun start() {
    log.info("Launching indexer ...")

    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    metaIndexerFactories = FilteredServiceLoader.load(MetaIndexerFactory::class.java).toList()
    indexerFactories = FilteredServiceLoader.load(IndexerFactory::class.java).toList()

    index = MongoDBIndex.create(vertx)
    store = StoreFactory.createStore(vertx)

    registerMessageConsumers()
  }

  override suspend fun stop() {
    index.close()
    store.close()
  }

  /**
   * Register all message consumers for this verticle
   */
  private fun registerMessageConsumers() {
    registerQuery()
  }

  /**
   * Register consumer for queries
   */
  private fun registerQuery() {
    vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_QUERY) { msg ->
      launch {
        try {
          val result = onQuery(msg.body())
          msg.reply(result)
        } catch (t: Throwable) {
          log.error("Could not perform query", t)
          msg.fail(throwableToCode(t), throwableToMessage(t, ""))
        }
      }
    }
  }

  /**
   * Handle a query
   */
  private suspend fun onQuery(body: JsonObject): JsonObject {
    val search = body.getString("search") ?: ""
    val path = body.getString("path")

    val query = DefaultQueryCompiler(metaIndexerFactories + indexerFactories).compileQuery(search, path)

    val hits = index.getMeta(query)

    // TODO implement scrolling/paging

    return json {
      obj(
        "totalHits" to hits.size,
        "hits" to hits
      )
    }
  }
}
