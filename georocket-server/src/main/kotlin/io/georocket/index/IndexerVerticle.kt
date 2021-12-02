package io.georocket.index

import io.georocket.constants.AddressConstants
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.tasks.IndexingTask
import io.georocket.tasks.RemovingTask
import io.georocket.tasks.TaskError
import io.georocket.util.FilteredServiceLoader
import io.georocket.util.ThrowableHelper.throwableToCode
import io.georocket.util.ThrowableHelper.throwableToMessage
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import java.time.Instant

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
    registerDelete()
    registerQuery()
  }

  /**
   * Register consumer for delete messages
   */
  private fun registerDelete() {
    vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_DELETE) { msg ->
      launch {
        try {
          onDelete(msg.body())
          msg.reply(null)
        } catch (t: Throwable) {
          log.error("Could not delete document", t)
          msg.fail(throwableToCode(t), throwableToMessage(t, ""))
        }
      }
    }
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
   * Send indexer tasks for the correlation IDs in the given messages
   * to the task verticle
   * @param messages the messages
   * @param incIndexedChunks if the number of indexed chunks should be increased
   */
  private fun startIndexerTasks(messages: List<Message<JsonObject>>,
      incIndexedChunks: Boolean = false) {
    var currentTask: IndexingTask? = null

    for (msg in messages) {
      val body = msg.body()
      val correlationId = body.getString("correlationId")
      if (currentTask == null) {
        currentTask = IndexingTask(correlationId)
        currentTask.startTime = Instant.now()
      } else if (currentTask.correlationId != correlationId) {
        vertx.eventBus().publish(AddressConstants.TASK_INC,
            JsonObject.mapFrom(currentTask))
        currentTask = IndexingTask(correlationId)
        currentTask.startTime = Instant.now()
      }

      if (incIndexedChunks) {
        currentTask.indexedChunks = currentTask.indexedChunks + 1
      }
    }

    if (currentTask != null) {
      vertx.eventBus().publish(AddressConstants.TASK_INC,
          JsonObject.mapFrom(currentTask))
    }
  }

  /**
   * Send indexer tasks to the task verticle and accumulate the number of
   * indexed chunks for the correlation IDs in the given messages
   * @param messages the messages
   */
  private fun updateIndexerTasks(messages: List<Message<JsonObject>>) {
    startIndexerTasks(messages, true)
  }

  /**
   * Send a message to the task verticle telling it that we are now starting
   * to remove chunks from the index
   * @param correlationId the correlation ID of the removing task
   * @param totalChunks the total number of chunks to remove
   */
  private fun startRemovingTask(correlationId: String?, totalChunks: Long) {
    if (correlationId == null) {
      return
    }
    val removingTask = RemovingTask(correlationId)
    removingTask.startTime = Instant.now()
    removingTask.totalChunks = totalChunks
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(removingTask))
  }

  /**
   * Send a message to the task verticle telling it that we just removed the
   * given number of chunks from the index
   * @param correlationId the correlation ID of the removing task
   * @param error an error that occurred during the task execution (may be
   * `null` if everything is OK
   */
  private fun updateRemovingTask(correlationId: String?, removedChunks: Int,
      error: TaskError?) {
    if (correlationId == null) {
      return
    }
    val removingTask = RemovingTask(correlationId)
    removingTask.removedChunks = removedChunks.toLong()
    if (error != null) {
      removingTask.addError(error)
    }
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(removingTask))
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

  /**
   * Delete chunks from the index
   * @param body the message containing the paths to the chunks to delete
   * @return a Completable that completes when the chunks have been deleted
   * successfully
   */
  private suspend fun onDelete(body: JsonObject) {
    val paths = body.getJsonArray("paths")
    val correlationId = body.getString("correlationId")
    val totalChunks = body.getLong("totalChunks", paths.size().toLong())
    val remainingChunks = body.getLong("remainingChunks", paths.size().toLong())

    if (paths.size() < remainingChunks) {
      log.info("Deleting ${paths.size()}/$remainingChunks chunks from index ...")
    } else {
      log.info("Deleting ${paths.size()} chunks from index ...")
    }

    startRemovingTask(correlationId, totalChunks)

    // execute bulk request
    val startTimeStamp = System.currentTimeMillis()
    index.delete(paths.toList().map { it.toString() })

    val stopTimeStamp = System.currentTimeMillis()
    // TODO handle errors
    // if (client.bulkResponseHasErrors(bres)) {
    //   val error = client.bulkResponseGetErrorMessage(bres)
    //   log.error("One or more chunks could not be deleted")
    //   log.error(error)
    //   updateRemovingTask(correlationId, paths.size(),
    //       TaskError("generic_error", error))
    //   throw IllegalStateException("One or more chunks could not be deleted")
    // } else {
      log.info("Finished deleting ${paths.size()} chunks from index in "
          + (stopTimeStamp - startTimeStamp) + " ms")
      updateRemovingTask(correlationId, paths.size(), null)
    // }
  }
}
