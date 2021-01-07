package io.georocket.index

import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.index.elasticsearch.ElasticsearchClient
import io.georocket.index.elasticsearch.ElasticsearchClientFactory
import io.georocket.index.generic.DefaultMetaIndexerFactory
import io.georocket.index.xml.JsonIndexerFactory
import io.georocket.index.xml.MetaIndexerFactory
import io.georocket.index.xml.StreamIndexer
import io.georocket.index.xml.XMLIndexerFactory
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.ChunkMeta
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.IndexMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.RxStore
import io.georocket.storage.LegacyStoreFactory
import io.georocket.storage.XMLChunkMeta
import io.georocket.tasks.IndexingTask
import io.georocket.tasks.RemovingTask
import io.georocket.tasks.TaskError
import io.georocket.util.FilteredServiceLoader
import io.georocket.util.JsonParserTransformer
import io.georocket.util.MapUtils
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.RxUtils
import io.georocket.util.StreamEvent
import io.georocket.util.ThrowableHelper.throwableToCode
import io.georocket.util.ThrowableHelper.throwableToMessage
import io.georocket.util.XMLParserTransformer
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.rx.java.RxHelper
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.Message
import org.jooq.lambda.tuple.Tuple
import org.jooq.lambda.tuple.Tuple3
import rx.Completable
import rx.Observable
import rx.Single
import rx.functions.Func1
import java.time.Instant
import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * Generic methods for background indexing of any messages
 * @author Michel Kraemer
 */
class IndexerVerticle : AbstractVerticle() {
  companion object {
    private val log = LoggerFactory.getLogger(IndexerVerticle::class.java)

    private const val BUFFER_TIMESPAN: Long = 5000
    private const val MAX_RETRIES = 5
    private const val RETRY_INTERVAL = 1000

    /**
     * Elasticsearch index
     */
    private const val INDEX_NAME = "georocket"

    /**
     * Type of documents stored in the Elasticsearch index
     */
    private const val TYPE_NAME = "object"
  }

  /**
   * The Elasticsearch client
   */
  private lateinit var client: ElasticsearchClient

  /**
   * The GeoRocket store
   */
  private lateinit var store: RxStore

  /**
   * Compiles search strings to Elasticsearch documents
   */
  private lateinit var queryCompiler: DefaultQueryCompiler

  /**
   * A list of [IndexerFactory] objects
   */
  private lateinit var indexerFactories: List<IndexerFactory>

  /**
   * A view on [indexerFactories] containing only [XMLIndexerFactory] objects
   */
  private lateinit var xmlIndexerFactories: List<XMLIndexerFactory>

  /**
   * A view on [indexerFactories] containing only [JsonIndexerFactory] objects
   */
  private lateinit var jsonIndexerFactories: List<JsonIndexerFactory>

  /**
   * A view on [indexerFactories] containing only [MetaIndexerFactory] objects
   */
  private lateinit var metaIndexerFactories: List<MetaIndexerFactory>

  /**
   * The maximum number of chunks to index in one bulk
   */
  private var maxBulkSize = 0

  /**
   * The maximum number of bulk processes to run in parallel. Also affects the
   * number of parallel bulk inserts into Elasticsearch.
   */
  private var maxParallelInserts = 0

  /**
   * The maximum number of chunks the indexer queues due to backpressure before
   * it tells the importer to pause (see [.queuedAddMessages]). If this
   * happens, the indexer will later unpause the importer as soon as at least
   * half of the queued chunks have been indexed.
   */
  private var maxQueuedChunks = 0

  /**
   * The number of add message currently queued due to backpressure
   * (see [.onAdd])
   */
  private var queuedAddMessages = 0

  /**
   * `true` if the importer is currently paused due to backpressure
   */
  private var pauseImport = false

  override fun start(startFuture: Future<Void>) {
    log.info("Launching indexer ...")

    maxBulkSize = config().getInteger(ConfigConstants.INDEX_MAX_BULK_SIZE,
        ConfigConstants.DEFAULT_INDEX_MAX_BULK_SIZE)
    maxParallelInserts = config().getInteger(ConfigConstants.INDEX_MAX_PARALLEL_INSERTS,
        ConfigConstants.DEFAULT_INDEX_MAX_PARALLEL_INSERTS)
    maxQueuedChunks = config().getInteger(ConfigConstants.INDEX_MAX_QUEUED_CHUNKS,
        ConfigConstants.DEFAULT_INDEX_MAX_QUEUED_CHUNKS)

    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    indexerFactories = FilteredServiceLoader.load(IndexerFactory::class.java).toList()
    xmlIndexerFactories = indexerFactories.filterIsInstance<XMLIndexerFactory>()
    jsonIndexerFactories = indexerFactories.filterIsInstance<JsonIndexerFactory>()
    metaIndexerFactories = indexerFactories.filterIsInstance<MetaIndexerFactory>()

    store = RxStore(LegacyStoreFactory.createStore(getVertx()))

    queryCompiler = createQueryCompiler()
    queryCompiler.setQueryCompilers(indexerFactories)

    ElasticsearchClientFactory(vertx).createElasticsearchClient(INDEX_NAME)
        .doOnSuccess { es -> client = es }
        .flatMapCompletable { client.ensureIndex() }
        .andThen(Completable.defer { ensureMapping() })
        .subscribe({
          registerMessageConsumers()
          startFuture.complete()
        }) { cause -> startFuture.fail(cause) }
  }

  private fun createQueryCompiler(): DefaultQueryCompiler {
    val config = vertx.orCreateContext.config()
    val cls = config.getString(ConfigConstants.QUERY_COMPILER_CLASS, DefaultQueryCompiler::class.java.name)
    return try {
      Class.forName(cls).newInstance() as DefaultQueryCompiler
    } catch (e: ReflectiveOperationException) {
      throw RuntimeException("Could not create a DefaultQueryCompiler", e)
    }
  }

  override fun stop() {
    client.close()
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
   * @return a function that can be passed to [Observable.retryWhen]
   * @see RxUtils.makeRetry
   */
  private fun makeRetry(): Func1<Observable<out Throwable>, Observable<Long>> {
    return RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log)
  }

  /**
   * Register consumer for add messages
   */
  private fun registerAdd() {
    vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_ADD)
        .toObservable()
        .doOnNext {
          queuedAddMessages++

          // pause import if necessary
          if (queuedAddMessages > maxQueuedChunks && !pauseImport) {
            pauseImport = true
            vertx.eventBus().send(AddressConstants.IMPORTER_PAUSE, pauseImport)
          }
        }
        .buffer(BUFFER_TIMESPAN, TimeUnit.MILLISECONDS, maxBulkSize)
        .onBackpressureBuffer() // unlimited buffer
        .flatMapCompletable({ messages ->
          queuedAddMessages -= messages.size

          // resume import if possible
          if (pauseImport && queuedAddMessages <= maxQueuedChunks / 2) {
            pauseImport = false
            vertx.eventBus().send(AddressConstants.IMPORTER_PAUSE, pauseImport)
          }

          onAdd(messages).onErrorComplete { err ->
            // reply with error to all peers
            log.error("Could not index document", err)
            messages.forEach { it.fail(throwableToCode(err), err.message) }
            true
          }
        }, false, maxParallelInserts)
        .toCompletable()
        .subscribe({
          // ignore
        }) { err ->
          // This is bad. It will unsubscribe the consumer from the eventbus!
          // Should never happen anyhow. If it does, something else has
          // completely gone wrong.
          log.fatal("Could not index document", err)
        }
  }

  /**
   * Register consumer for delete messages
   */
  private fun registerDelete() {
    vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_DELETE)
        .toObservable()
        .subscribe { msg ->
          onDelete(msg.body()).subscribe({
            msg.reply(null)
          }) { err ->
            log.error("Could not delete document", err)
            msg.fail(throwableToCode(err), throwableToMessage(err, ""))
          }
        }
  }

  /**
   * Register consumer for queries
   */
  private fun registerQuery() {
    vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_QUERY)
        .toObservable()
        .subscribe { msg ->
          onQuery(msg.body()).subscribe({
            msg.reply(it)
          }) { err ->
            log.error("Could not perform query", err)
            msg.fail(throwableToCode(err), throwableToMessage(err, ""))
          }
        }
  }

  private fun ensureMapping(): Completable {
    // merge mappings from all indexers
    val mappings: Map<String, Any> = HashMap()
    indexerFactories.filterIsInstance<DefaultMetaIndexerFactory>()
        .forEach { factory -> MapUtils.deepMerge(mappings, factory.mapping) }
    indexerFactories.filter { it !is DefaultMetaIndexerFactory }
        .forEach { factory -> MapUtils.deepMerge(mappings, factory.mapping) }

    return client.putMapping(TYPE_NAME, JsonObject(mappings)).toCompletable()
  }

  /**
   * Insert multiple Elasticsearch documents into the index. Perform a
   * bulk request. This method replies to all messages if the bulk request
   * was successful.
   * @param documents a list of tuples containing document IDs, documents to
   * index, and the respective messages from which the documents were created
   * @return a Completable that completes when the operation has finished
   */
  private fun insertDocuments(documents: List<Tuple3<String, JsonObject, Message<JsonObject>>>): Completable {
    val startTimeStamp = System.currentTimeMillis()

    val chunkPaths = documents.map { it.v1 }

    if (queuedAddMessages > 0) {
      val total = chunkPaths.size + queuedAddMessages
      log.info("Indexing ${chunkPaths.size}/$total chunks")
    } else {
      log.info("Indexing ${chunkPaths.size} chunks")
    }

    val docsToInsert = documents.map { it.limit2() }
    val messages = documents.map { it.v3 }

    return client.bulkInsert(TYPE_NAME, docsToInsert).flatMapCompletable { bres ->
      val items = bres.getJsonArray("items")
      for (i in 0 until items.size()) {
        val jo = items.getJsonObject(i)
        val item = jo.getJsonObject("index")
        val msg = messages[i]
        if (client.bulkResponseItemHasErrors(item)) {
          msg.fail(500, client.bulkResponseItemGetErrorMessage(item))
        } else {
          msg.reply(null)
        }
      }

      val stopTimeStamp = System.currentTimeMillis()
      val errorMessage = client.bulkResponseGetErrorMessage(bres)
      if (errorMessage != null) {
        log.error("Indexing failed")
        log.error(errorMessage)
      } else {
        log.info("Finished indexing ${chunkPaths.size} chunks in " +
            (stopTimeStamp - startTimeStamp) + " ms")
      }

      Completable.complete()
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
   * Get a chunk from the store but first look into the cache of indexable chunks
   * @param path the chunk's path
   * @return the chunk
   */
  private fun getChunkFromStore(path: String): Single<ChunkReadStream> {
    val chunk = IndexableChunkCache.getInstance()[path]
    return if (chunk != null) {
      Single.just(DelegateChunkReadStream(chunk))
    } else {
      store.rxGetOne(path)
    }
  }

  /**
   * Open a chunk and convert it to an Elasticsearch document. Retry operation
   * several times before failing.
   * @param path the path to the chunk to open
   * @param chunkMeta metadata about the chunk
   * @param indexMeta metadata used to index the chunk
   * @return an observable that emits the document
   */
  private fun openChunkToDocument(path: String, chunkMeta: ChunkMeta,
      indexMeta: IndexMeta): Observable<Map<String, Any>> {
    return Observable.defer {
      getChunkFromStore(path)
          .flatMapObservable { chunk ->
            val factories: List<IndexerFactory>
            val parserTransformer: Observable.Transformer<Buffer, out StreamEvent>

            // select indexers and parser depending on the mime type
            val mimeType = chunkMeta.mimeType
            if (belongsTo(mimeType, "application", "xml") ||
                belongsTo(mimeType, "text", "xml")) {
              factories = xmlIndexerFactories
              parserTransformer = XMLParserTransformer()
            } else if (belongsTo(mimeType, "application", "json")) {
              factories = jsonIndexerFactories
              parserTransformer = JsonParserTransformer()
            } else {
              return@flatMapObservable Observable.error<Map<String, Any>>(
                  NoStackTraceThrowable("Unexpected mime type '${mimeType}' " +
                      "while trying to index chunk '$path'"))
            }

            // call meta indexers
            val metaResults: MutableMap<String, Any> = HashMap()
            for (metaIndexerFactory in metaIndexerFactories) {
              val metaIndexer = metaIndexerFactory.createIndexer()
              metaIndexer.onIndexChunk(path, chunkMeta, indexMeta)
              metaResults.putAll(metaIndexer.result)
            }

            chunkToDocument(chunk, indexMeta.fallbackCRSString, parserTransformer, factories)
                .doAfterTerminate { chunk.close() }
                // add results from meta indexers to converted document
                .doOnNext { it.putAll(metaResults) }
          }
    }.retryWhen(makeRetry())
  }

  /**
   * Convert a chunk to a Elasticsearch document
   * @param chunk the chunk to convert
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   * @param parserTransformer the transformer used to parse the chunk stream
   * into stream events
   * @param indexerFactories a sequence of indexer factories that should be
   * used to index the chunk
   * @return an observable that will emit the document
   */
  private fun <T : StreamEvent> chunkToDocument(chunk: ChunkReadStream,
      fallbackCRSString: String?, parserTransformer: Observable.Transformer<Buffer, T>,
      indexerFactories: List<IndexerFactory>): Observable<MutableMap<String, Any>> {
    val indexers: MutableList<StreamIndexer<T>> = ArrayList()
    indexerFactories.forEach { factory ->
      val i = factory.createIndexer() as StreamIndexer<T>
      if (fallbackCRSString != null && i is CRSAware) {
        i.setFallbackCRSString(fallbackCRSString)
      }
      indexers.add(i)
    }

    return RxHelper.toObservable(chunk)
        .compose(parserTransformer)
        .doOnNext { e -> indexers.forEach { it.onEvent(e) } }
        .last() // "wait" until the whole chunk has been consumed
        .map {
          // create the Elasticsearch document
          val doc: MutableMap<String, Any> = HashMap()
          indexers.forEach(Consumer { i -> doc.putAll(i.result) })
          doc
        }
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
   * @param messages the list of add messages that contain the paths to
   * the chunks to be indexed
   * @return a Completable that completes when the operation has finished
   */
  private fun onAdd(messages: List<Message<JsonObject>>): Completable {
    startIndexerTasks(messages)
    return Observable.from(messages)
        .flatMap { msg ->
          // get path to chunk from message
          val body = msg.body()
          val path = body.getString("path")
          if (path == null) {
            msg.fail(400, "Missing path to the chunk to index")
            return@flatMap Observable.empty()
          }

          // get chunk metadata
          val meta = body.getJsonObject("meta")
          if (meta == null) {
            msg.fail(400, "Missing metadata for chunk $path")
            return@flatMap Observable.empty()
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

          openChunkToDocument(path, chunkMeta, indexMeta)
              .map { doc -> Tuple.tuple(path, JsonObject(doc), msg) }
              .onErrorResumeNext { err ->
                msg.fail(throwableToCode(err), throwableToMessage(err, ""))
                Observable.empty()
              }
        }
        .toList()
        .flatMapCompletable { l ->
          if (l.isNotEmpty()) {
            return@flatMapCompletable insertDocuments(l)
          }
          Completable.complete()
        }
        .toCompletable()
        .doOnCompleted { updateIndexerTasks(messages) }
  }

  /**
   * Write result of a query given the Elasticsearch response
   * @param body the message containing the query
   * @return an observable that emits the results of the query
   */
  private fun onQuery(body: JsonObject): Single<JsonObject> {
    val search = body.getString("search")
    val path = body.getString("path")
    val scrollId = body.getString("scrollId")
    val pageSize = body.getInteger("size", 100)
    val timeout = "1m" // one minute
    val parameters = JsonObject().put("size", pageSize)

    // We only need the chunk meta. Exclude all other source fields.
    parameters.put("_source", "chunkMeta")

    val single = if (scrollId == null) {
      // Execute a new search. Use a post_filter because we only want to get
      // a yes/no answer and no scoring (i.e. we only want to get matching
      // documents and not those that likely match). For the difference between
      // query and post_filter see the Elasticsearch documentation.
      val postFilter = try {
        queryCompiler.compileQuery(search, path)
      } catch (t: Throwable) {
        return Single.error(t)
      }
      client.beginScroll(TYPE_NAME, null, postFilter, parameters, timeout)
    } else {
      // continue searching
      client.continueScroll(scrollId, timeout)
    }

    return single.map { sr ->
      // iterate through all hits and convert them to JSON
      val hits = sr.getJsonObject("hits")
      val totalHits = hits.getLong("total")
      val resultHits = JsonArray()
      val hitsHits = hits.getJsonArray("hits")
      for (o in hitsHits) {
        val hit = o as JsonObject
        val id = hit.getString("_id")
        val source = hit.getJsonObject("_source")
        val jsonMeta = source.getJsonObject("chunkMeta")
        val meta = getMeta(jsonMeta)
        val obj = meta.toJsonObject().put("id", id)
        resultHits.add(obj)
      }

      JsonObject()
          .put("totalHits", totalHits)
          .put("hits", resultHits)
          .put("scrollId", sr.getString("_scroll_id"))
    }
  }

  /**
   * Delete chunks from the index
   * @param body the message containing the paths to the chunks to delete
   * @return a Completable that completes when the chunks have been deleted
   * successfully
   */
  private fun onDelete(body: JsonObject): Completable {
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
    return client.bulkDelete(TYPE_NAME, paths).flatMapCompletable { bres ->
      val stopTimeStamp = System.currentTimeMillis()
      if (client.bulkResponseHasErrors(bres)) {
        val error = client.bulkResponseGetErrorMessage(bres)
        log.error("One or more chunks could not be deleted")
        log.error(error)
        updateRemovingTask(correlationId, paths.size(),
            TaskError("generic_error", error))
        return@flatMapCompletable Completable.error(NoStackTraceThrowable(
            "One or more chunks could not be deleted"))
      } else {
        log.info("Finished deleting ${paths.size()} chunks from index in "
            + (stopTimeStamp - startTimeStamp) + " ms")
        updateRemovingTask(correlationId, paths.size(), null)
        return@flatMapCompletable Completable.complete()
      }
    }
  }
}
