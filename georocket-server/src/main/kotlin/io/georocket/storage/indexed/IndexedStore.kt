package io.georocket.storage.indexed

import io.georocket.constants.AddressConstants
import io.georocket.storage.DeleteMeta
import io.georocket.storage.Store
import io.georocket.storage.StoreCursor
import io.georocket.tasks.PurgingTask
import io.georocket.tasks.TaskError
import io.vertx.core.Vertx
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import java.time.Instant
import java.util.ArrayDeque

/**
 * An abstract base class for chunk stores that are backed by an indexer
 * @author Michel Kraemer
 */
abstract class IndexedStore(private val vertx: Vertx) : Store {
  /**
   * Send a message to the task verticle telling it that we are now starting
   * to delete [totalChunks] chunks from the store
   */
  private fun startPurgingTask(correlationId: String, totalChunks: Long) {
    val purgingTask = PurgingTask(correlationId)
    purgingTask.startTime = Instant.now()
    purgingTask.totalChunks = totalChunks
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(purgingTask))
  }

  /**
   * Send a message to the task verticle telling it that we have finished
   * deleting chunks from the store
   */
  private fun stopPurgingTask(correlationId: String, error: TaskError?) {
    val purgingTask = PurgingTask(correlationId)
    purgingTask.endTime = Instant.now()
    if (error != null) {
      purgingTask.addError(error)
    }
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(purgingTask))
  }

  /**
   * Send a message to the task verticle telling it that we just deleted
   * [purgedChunks] chunks from the store
   * @param correlationId the correlation ID of the purging task
   */
  private fun updatePurgingTask(correlationId: String, purgedChunks: Long) {
    val purgingTask = PurgingTask(correlationId)
    purgingTask.purgedChunks = purgedChunks
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(purgingTask))
  }

  override suspend fun delete(search: String?, path: String, deleteMetadata: DeleteMeta) {
    val correlationId = deleteMetadata.correlationId
    if (correlationId != null) {
      startPurgingTask(correlationId, 0)
    }

    var cause: Throwable? = null
    try {
      val cursor = get(search, path)
      if (correlationId != null) {
        startPurgingTask(correlationId, cursor.info.totalHits)
      }
      doDelete(cursor, correlationId)
    } catch (t: Throwable) {
      if (t !is ReplyException || t.failureCode() != 404) {
        cause = t
      }
    } finally {
      if (correlationId != null) {
        stopPurgingTask(correlationId, cause?.let { TaskError(it) })
      }
      if (cause != null) {
        throw cause
      }
    }
  }

  override suspend fun get(search: String?, path: String): StoreCursor {
    return IndexedStoreCursor(vertx, search, path).start()
  }

  override suspend fun scroll(search: String?, path: String, size: Int): StoreCursor {
    return FrameCursor(vertx, search, path, size).start()
  }

  override suspend fun scroll(scrollId: String): StoreCursor {
    return FrameCursor(vertx, scrollId = scrollId).start()
  }

  /**
   * Iterate over a [cursor] and delete all returned chunks from the index
   * and the store
   */
  private suspend fun doDelete(cursor: StoreCursor, correlationId: String?) {
    var remainingChunks = cursor.info.totalHits
    val paths = ArrayDeque<String>()

    while (cursor.hasNext()) {
      cursor.next()

      // add item to queue
      paths.add(cursor.chunkPath)
      val size = cursor.info.currentHits.toLong()

      if (paths.size >= size) {
        // if there are enough items in the queue, bulk delete them
        doDeleteBulk(paths, cursor.info.totalHits, remainingChunks, correlationId)
        paths.clear()
        correlationId?.let { updatePurgingTask(it, size) }
        remainingChunks -= size
      }
    }

    if (!paths.isEmpty()) {
      // bulk delete the remaining ones
      doDeleteBulk(paths, cursor.info.totalHits, remainingChunks, correlationId)
    }
  }

  /**
   * Delete all chunks with the given [paths] from the index and from the store
   */
  private suspend fun doDeleteBulk(paths: Iterable<String>, totalChunks: Long,
      remainingChunks: Long, correlationId: String?) {
    // delete from index first so the chunks cannot be found anymore
    val indexMsg = JsonObject()
        .put("correlationId", correlationId)
        .put("paths", JsonArray(paths.toList()))
        .put("totalChunks", totalChunks)
        .put("remainingChunks", remainingChunks)
    vertx.eventBus().requestAwait<Any>(AddressConstants.INDEXER_DELETE, indexMsg)

    // now delete all chunks from the store
    doDeleteChunks(paths)
  }

  override suspend fun getAttributeValues(search: String?, path: String,
      attribute: String): List<Any?> {
    val msg = JsonObject()
      .put("search", search)
      .put("attribute", attribute)
      .put("path", path)
    return vertx.eventBus().requestAwait<JsonArray>(
      AddressConstants.METADATA_GET_ATTRIBUTE_VALUES, msg).body().toList()
  }

  override suspend fun getPropertyValues(search: String?, path: String,
      property: String): List<Any?> {
    val msg = JsonObject()
        .put("search", search)
        .put("property", property)
        .put("path", path)
    return vertx.eventBus().requestAwait<JsonArray>(
      AddressConstants.METADATA_GET_PROPERTY_VALUES, msg).body().toList()
  }

  override suspend fun setProperties(search: String?, path: String,
      properties: Map<String, Any>) {
    val msg = JsonObject()
        .put("search", search)
        .put("properties", JsonObject.mapFrom(properties))
        .put("path", path)
    vertx.eventBus().requestAwait<Any>(AddressConstants.METADATA_SET_PROPERTIES, msg)
  }

  override suspend fun removeProperties(search: String?, path: String,
      properties: List<String>) {
    val msg = JsonObject()
        .put("search", search)
        .put("properties", JsonArray(properties))
        .put("path", path)
    vertx.eventBus().requestAwait<Any>(AddressConstants.METADATA_REMOVE_PROPERTIES, msg)
  }

  override suspend fun appendTags(search: String?, path: String, tags: List<String>) {
    val msg = JsonObject()
        .put("search", search)
        .put("tags", JsonArray(tags))
        .put("path", path)
    vertx.eventBus().requestAwait<Any>(AddressConstants.METADATA_APPEND_TAGS, msg)
  }

  override suspend fun removeTags(search: String?, path: String, tags: List<String>) {
    val msg = JsonObject()
        .put("search", search)
        .put("tags", JsonArray(tags))
        .put("path", path)
    vertx.eventBus().requestAwait<Any>(AddressConstants.METADATA_REMOVE_TAGS, msg)
  }

  /**
   * Delete all chunks with the given paths from the store
   */
  protected abstract suspend fun doDeleteChunks(paths: Iterable<String>)
}
