package io.georocket.index

import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.index.geojson.JsonTransformer
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.index.xml.XMLTransformer
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.ChunkMeta
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.IndexMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.storage.XMLChunkMeta
import io.georocket.tasks.IndexingTask
import io.georocket.tasks.RemovingTask
import io.georocket.tasks.TaskError
import io.georocket.util.FilteredServiceLoader
import io.georocket.util.JsonStreamEvent
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.StreamEvent
import io.georocket.util.ThrowableHelper.throwableToCode
import io.georocket.util.ThrowableHelper.throwableToMessage
import io.georocket.util.XMLStreamEvent
import io.georocket.util.debounce
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.jooq.lambda.tuple.Tuple
import org.jooq.lambda.tuple.Tuple3
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

  /**
   * The maximum number of chunks to index in one bulk
   */
  private var maxBulkSize = 0

  override suspend fun start() {
    log.info("Launching indexer ...")

    maxBulkSize = config.getInteger(ConfigConstants.INDEX_MAX_BULK_SIZE,
        ConfigConstants.DEFAULT_INDEX_MAX_BULK_SIZE)

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
    registerAdd()
    registerDelete()
    registerQuery()
  }

  /**
   * Register consumer for add messages
   */
  private fun registerAdd() {
    val queue = ArrayDeque<Message<JsonObject>>()

    val doAdd: suspend () -> Unit = {
      val toAdd = mutableListOf<Message<JsonObject>>()
      while (!queue.isEmpty()) {
        toAdd.add(queue.removeFirst())
      }
      onAdd(toAdd)
    }

    val onBulkAdd = debounce(vertx) { doAdd() }

    vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_ADD) { msg ->
      queue.add(msg)
      // TODO make chunk size configurable?
      if (queue.size >= 200) {
        launch {
          doAdd()
        }
      } else {
        onBulkAdd()
      }
    }
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
   * Insert multiple documents into the index. Perform a bulk request. This
   * method replies to all messages when the bulk request has finished.
   * @param documents a list of tuples containing document IDs, documents to
   * index, and the respective messages from which the documents were created
   */
  private suspend fun insertDocuments(documents: List<Tuple3<String, JsonObject, Message<JsonObject>>>) {
    val startTimeStamp = System.currentTimeMillis()

    val chunkPaths = documents.map { it.v1 }

    log.info("Indexing ${chunkPaths.size} chunks")

    index.addMany(documents.map { it.v1 to it.v2 })
    for (d in documents) {
      // TODO handle exception and send it back to d.v3
      d.v3.reply(null)
    }

    // log error if one of the inserts failed
    val stopTimeStamp = System.currentTimeMillis()
    log.info("Finished indexing ${chunkPaths.size} chunks in " +
        (stopTimeStamp - startTimeStamp) + " ms")
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
   * Open a chunk and convert it to a document. Retry operation several times
   * before failing.
   * @param path the path to the chunk to open
   * @param chunkMeta metadata about the chunk
   * @param indexMeta metadata used to index the chunk
   * @return an observable that emits the document
   */
  private suspend fun openChunkToDocument(path: String, chunkMeta: ChunkMeta,
      indexMeta: IndexMeta): Map<String, Any> {
    val chunk = IndexableChunkCache.getInstance()[path] ?: store.getOne(path)

    // call meta indexers
    val metaResults = mutableMapOf<String, Any>()
    for (metaIndexerFactory in metaIndexerFactories) {
      val metaIndexer = metaIndexerFactory.createIndexer()
      val metaResult = metaIndexer.onChunk(path, chunkMeta, indexMeta)
      metaResults.putAll(metaResult)
    }

    // index chunks depending on the mime type
    val mimeType = chunkMeta.mimeType
    val doc = if (belongsTo(mimeType, "application", "xml") ||
        belongsTo(mimeType, "text", "xml")) {
      chunkToDocument(chunk, indexMeta.fallbackCRSString,
        XMLStreamEvent::class.java, XMLTransformer())
    } else if (belongsTo(mimeType, "application", "json")) {
      chunkToDocument(chunk, indexMeta.fallbackCRSString,
        JsonStreamEvent::class.java, JsonTransformer())
    } else {
      throw IllegalArgumentException("Unexpected mime type '${mimeType}' " +
              "while trying to index chunk '$path'")
    }

    // add results from meta indexers to converted document
    return doc + metaResults
  }

  /**
   * Convert a chunk to a document
   */
  private suspend fun <T : StreamEvent> chunkToDocument(chunk: Buffer,
      fallbackCRSString: String?, type: Class<T>, transformer: Transformer<T>): Map<String, Any> {
    // initialize indexers
    val indexers = indexerFactories.mapNotNull { factory ->
      factory.createIndexer(type)?.also { i ->
        if (fallbackCRSString != null && i is CRSAware) {
          i.setFallbackCRSString(fallbackCRSString)
        }
      }
    }

    // perform indexing
    transformer.transform(chunk).collect { e ->
      indexers.forEach { it.onEvent(e) }
    }

    // create the document
    val doc = mutableMapOf<String, Any>()
    indexers.forEach { indexer -> doc.putAll(indexer.makeResult()) }
    return doc
  }

  /**
   * Convert a [JsonObject] to a [ChunkMeta] object
   * @param source the JSON object to convert
   * @return the converted object
   */
  private fun getMeta(source: JsonObject): ChunkMeta {
    val mimeType = source.getString("mimeType", XMLChunkMeta.MIME_TYPE)
    return when {
      belongsTo(mimeType, "application", "xml") ||
          belongsTo(mimeType, "text", "xml") -> XMLChunkMeta(source)

      belongsTo(mimeType, "application", "geo+json") -> GeoJsonChunkMeta(source)

      belongsTo(mimeType, "application", "json") -> JsonChunkMeta(source)

      else -> ChunkMeta(source)
    }
  }

  /**
   * Will be called when chunks should be added to the index
   */
  private suspend fun onAdd(messages: List<Message<JsonObject>>) {
    startIndexerTasks(messages)
    try {
      val documents = messages.mapNotNull { msg ->
        // get path to chunk from message
        val body = msg.body()
        val path = body.getString("path")
        if (path == null) {
          msg.fail(400, "Missing path to the chunk to index")
          return@mapNotNull null
        }

        // get chunk metadata
        val meta = body.getJsonObject("meta")
        if (meta == null) {
          msg.fail(400, "Missing metadata for chunk $path")
          return@mapNotNull null
        }

        // get tags
        val tags = body.getJsonArray("tags")?.flatMap { o ->
          if (o != null) {
            listOf(o.toString())
          } else {
            emptyList()
          }
        }

        // get properties
        val properties = body.getJsonObject("properties")?.map

        // get fallback CRS
        val fallbackCRSString = body.getString("fallbackCRSString")

        log.trace("Indexing $path")

        val correlationId = body.getString("correlationId")
        val filename = body.getString("filename")
        val timestamp = body.getLong("timestamp", System.currentTimeMillis())

        val chunkMeta = getMeta(meta)
        val indexMeta = IndexMeta(correlationId, filename, timestamp,
            tags, properties, fallbackCRSString)

        try {
          val doc = openChunkToDocument(path, chunkMeta, indexMeta)
          Tuple.tuple(path, JsonObject(doc), msg)
        } catch (t: Throwable) {
          log.error("Could not index chunk", t)
          msg.fail(throwableToCode(t), throwableToMessage(t, ""))
          null
        }
      }

      if (documents.isNotEmpty()) {
        insertDocuments(documents)
      }
    } finally {
      updateIndexerTasks(messages)
    }
  }

  /**
   * Handle a query
   * @param body the message containing the query
   * @return an observable that emits the results of the query
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
