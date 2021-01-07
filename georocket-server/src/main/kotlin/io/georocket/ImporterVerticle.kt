package io.georocket

import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.index.xml.XMLCRSIndexer
import io.georocket.input.Splitter
import io.georocket.input.geojson.GeoJsonSplitter
import io.georocket.input.xml.FirstLevelSplitter
import io.georocket.input.xml.XMLSplitter
import io.georocket.storage.ChunkMeta
import io.georocket.storage.IndexMeta
import io.georocket.storage.RxStore
import io.georocket.storage.StoreFactory
import io.georocket.tasks.ImportingTask
import io.georocket.tasks.TaskError
import io.georocket.util.JsonParserTransformer
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.RxUtils
import io.georocket.util.StringWindow
import io.georocket.util.UTF8BomFilter
import io.georocket.util.Window
import io.georocket.util.XMLParserTransformer
import io.georocket.util.io.RxGzipReadStream
import io.vertx.core.file.OpenOptions
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.buffer.Buffer
import io.vertx.rxjava.core.eventbus.Message
import io.vertx.rxjava.core.file.AsyncFile
import io.vertx.rxjava.core.streams.ReadStream
import rx.Completable
import rx.Observable
import rx.Single
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

/**
 * Imports file in the background
 * @author Michel Kraemer
 */
class ImporterVerticle : AbstractVerticle() {
  companion object {
    private val log = LoggerFactory.getLogger(ImporterVerticle::class.java)

    private const val MAX_RETRIES = 5
    private const val RETRY_INTERVAL = 1000
    private const val MAX_PARALLEL_IMPORTS = 1
    private const val MAX_PARALLEL_ADDS = 10
  }

  private lateinit var store: RxStore
  private lateinit var incoming: String
  private var paused = false
  private val filesBeingImported = mutableSetOf<AsyncFile>()

  override fun start() {
    log.info("Launching importer ...")

    store = RxStore(StoreFactory.createStore(getVertx()))
    val storagePath = config().getString(ConfigConstants.STORAGE_FILE_PATH)
    incoming = "$storagePath/incoming"

    vertx.eventBus().localConsumer<JsonObject>(AddressConstants.IMPORTER_IMPORT)
        .toObservable()
        .onBackpressureBuffer() // unlimited buffer
        .flatMapCompletable({ msg ->
          // call onImport() but ignore errors. onImport() will handle errors for us.
          onImport(msg).onErrorComplete()
        }, false, MAX_PARALLEL_IMPORTS)
        .subscribe({
          // ignore
        }) { err ->
          // This is bad. It will unsubscribe the consumer from the eventbus!
          // Should never happen anyhow. If it does, something else has
          // completely gone wrong.
          log.fatal("Could not import file", err)
        }

    vertx.eventBus().localConsumer(AddressConstants.IMPORTER_PAUSE) { msg: Message<Boolean> -> onPause(msg) }
  }

  /**
   * Receives a name of a file to import
   * @param msg the event bus message containing the filename
   * @return a Completable that will complete when the file has been imported
   */
  private fun onImport(msg: Message<JsonObject>): Completable {
    val body = msg.body()
    val filename = body.getString("filename")
    val filepath = "$incoming/$filename"
    val layer = body.getString("layer", "/")
    val contentType = body.getString("contentType")
    val correlationId = body.getString("correlationId")
    val fallbackCRSString = body.getString("fallbackCRSString")
    val contentEncoding = body.getString("contentEncoding")

    // get tags
    val tags: List<String>? = body.getJsonArray("tags")
      ?.filterNotNull()
      ?.map { it.toString() }
      ?.toList()

    // get properties
    val properties = body.getJsonObject("properties")?.map

    // generate timestamp for this import
    val timestamp = System.currentTimeMillis()

    log.info("Importing [$correlationId] to layer '$layer'")

    val fs = vertx.fileSystem()
    val openOptions = OpenOptions().setCreate(false).setWrite(false)
    return fs.rxOpen(filepath, openOptions)
        .flatMap { f ->
          filesBeingImported.add(f)
          importFile(contentType, f, correlationId, filename, timestamp,
              layer, tags, properties, fallbackCRSString, contentEncoding)
              .doAfterTerminate {
                // delete file from 'incoming' folder
                log.debug("Deleting $filepath from incoming folder")
                filesBeingImported.remove(f)
                f.rxClose()
                    .flatMap { fs.rxDelete(filepath) }
                    .subscribe({}) { log.error("Could not delete file from 'incoming' folder", it) }
              }
        }
        .doOnSuccess { chunkCount ->
          val duration = System.currentTimeMillis() - timestamp
          log.info("Finished importing [$correlationId] with $chunkCount" +
              " chunks to layer '$layer' after $duration ms")
        }
        .doOnError { err ->
          val duration = System.currentTimeMillis() - timestamp
          log.error("Failed to import [$correlationId] to layer '$layer'" +
              " after $duration ms", err)
        }
        .toCompletable()
  }

  /**
   * Import a file from the given read stream into the store. Inspect the file's
   * content type and forward to the correct import method.
   * @param contentType the file's content type
   * @param file the file to import
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored
   * @param tags the list of tags to attach to the file
   * @param properties the map of properties to attach to the file
   * @param fallbackCRSString the CRS which should be used if the imported
   * file does not specify one
   * @param contentEncoding the content encoding of the file to be
   * imported (e.g. "gzip").
   * @return a single that will emit with the number if chunks imported
   * when the file has been imported
   */
  private fun importFile(contentType: String, file: ReadStream<Buffer>,
      correlationId: String, filename: String, timestamp: Long, layer: String,
      tags: List<String>?, properties: Map<String, Any>?, fallbackCRSString: String?,
      contentEncoding: String?): Single<Int> {
    val f = if ("gzip" == contentEncoding) {
      log.debug("Importing file compressed with GZIP")
      RxGzipReadStream(file)
    } else if (contentEncoding != null && contentEncoding.isNotEmpty()) {
      log.warn("Unknown content encoding: `$contentEncoding'. Trying anyway.")
      file
    } else {
      file
    }

    // let the task verticle know that we're now importing
    val startTask = ImportingTask(correlationId)
    startTask.startTime = Instant.now()
    vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(startTask))

    val result = if (belongsTo(contentType, "application", "xml") ||
        belongsTo(contentType, "text", "xml")) {
      importXML(f, correlationId, filename, timestamp, layer, tags,
          properties, fallbackCRSString)
    } else if (belongsTo(contentType, "application", "json")) {
      importJSON(f, correlationId, filename, timestamp, layer, tags, properties)
    } else {
      Observable.error(NoStackTraceThrowable("Received an unexpected content " +
          "type '$contentType' while trying to import file '$filename'"))
    }

    val onFinish = Consumer { t: Throwable? ->
      // let the task verticle know that the import process has finished
      val endTask = ImportingTask(correlationId)
      endTask.endTime = Instant.now()
      if (t != null) {
        endTask.addError(TaskError(t))
      }
      vertx.eventBus().publish(AddressConstants.TASK_INC,
          JsonObject.mapFrom(endTask))
    }

    return result.window(100)
        .flatMap { it.count() }
        .doOnNext { n ->
          // let the task verticle know that we imported n chunks
          val currentTask = ImportingTask(correlationId)
          currentTask.importedChunks = n.toLong()
          vertx.eventBus().publish(AddressConstants.TASK_INC,
              JsonObject.mapFrom(currentTask))
        }
        .reduce(0, { a, b -> a + b })
        .toSingle()
        .doOnError { onFinish.accept(it) }
        .doOnSuccess { onFinish.accept(null) }
  }

  /**
   * Imports an XML file from the given input stream into the store
   * @param f the XML file to read
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored
   * @param tags the list of tags to attach to the file
   * @param properties the map of properties to attach to the file
   * @param fallbackCRSString the CRS which should be used if the imported
   * file does not specify one
   * @return an observable that will emit the number 1 when a chunk has been imported
   */
  private fun importXML(f: ReadStream<Buffer>, correlationId: String,
      filename: String, timestamp: Long, layer: String, tags: List<String>?,
      properties: Map<String, Any>?, fallbackCRSString: String?): Observable<Int> {
    val bomFilter = UTF8BomFilter()
    val window = Window()
    val splitter: XMLSplitter = FirstLevelSplitter(window)
    val processing = AtomicInteger(0)
    val crsIndexer = XMLCRSIndexer()
    return f.toObservable()
        .map { it.delegate }
        .map { bomFilter.filter(it) }
        .doOnNext { window.append(it) }
        .compose(XMLParserTransformer())
        .doOnNext { e ->
          // save the first CRS found in the file
          if (crsIndexer.crs == null) {
            crsIndexer.onEvent(e)
          }
        }
        .flatMap { splitter.onEventObservable(it) }
        .flatMapSingle({ result ->
          val crsString = crsIndexer.crs ?: fallbackCRSString
          val indexMeta = IndexMeta(correlationId, filename,
              timestamp, tags, properties, crsString)
          addToStoreWithPause(result, layer, indexMeta, f, processing)
              .toSingleDefault(1)
        }, false, MAX_PARALLEL_ADDS)
  }

  /**
   * Imports a JSON file from the given input stream into the store
   * @param f the JSON file to read
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored
   * @param tags the list of tags to attach to the file
   * @param properties the map of properties to attach to the file
   * @return an observable that will emit the number 1 when a chunk has been imported
   */
  private fun importJSON(f: ReadStream<Buffer>, correlationId: String,
      filename: String, timestamp: Long, layer: String, tags: List<String>?,
      properties: Map<String, Any>?): Observable<Int> {
    val bomFilter = UTF8BomFilter()
    val window = StringWindow()
    val splitter = GeoJsonSplitter(window)
    val processing = AtomicInteger(0)
    return f.toObservable()
        .map { it.delegate }
        .map { bomFilter.filter(it) }
        .doOnNext { window.append(it) }
        .compose(JsonParserTransformer())
        .flatMap { splitter.onEventObservable(it) }
        .flatMapSingle({ result ->
          val indexMeta = IndexMeta(correlationId, filename,
              timestamp, tags, properties, null)
          addToStoreWithPause(result, layer, indexMeta, f, processing)
              .toSingleDefault(1)
        }, false, MAX_PARALLEL_ADDS)
  }

  /**
   * Handle a pause message
   * @param msg the message
   */
  private fun onPause(msg: Message<Boolean>) {
    val paused = msg.body()
    if (paused == null || !paused) {
      if (this.paused) {
        log.info("Resuming import")
        this.paused = false
        for (f in filesBeingImported) {
          f.resume()
        }
      }
    } else {
      if (!this.paused) {
        log.info("Pausing import")
        this.paused = true
        for (f in filesBeingImported) {
          f.pause()
        }
      }
    }
  }

  /**
   * Add a chunk to the store. Pause the given read stream before adding and
   * increase the given counter. Decrease the counter after the chunk has been
   * written and only resume the read stream if the counter is `0`.
   * This is necessary because the writing to the store may take longer than
   * reading. We need to pause reading so the store is not overloaded (i.e.
   * we handle back-pressure here).
   * @param chunk the chunk to write
   * @param layer the layer the chunk should be added to
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @param f the read stream to pause while writing
   * @param processing an AtomicInteger keeping the number of chunks currently
   * being written (should be initialized to `0` the first time this
   * method is called)
   * @return a Completable that will complete when the operation has finished
   */
  private fun addToStoreWithPause(chunk: Splitter.Result<out ChunkMeta>,
      layer: String, indexMeta: IndexMeta, f: ReadStream<Buffer>,
      processing: AtomicInteger): Completable {
    // pause stream while chunk is being written
    f.pause()

    // count number of chunks being written
    processing.incrementAndGet()
    return addToStore(chunk.chunk, chunk.meta, layer, indexMeta)
        .doOnCompleted {
          // resume stream only after all chunks from the current
          // buffer have been stored
          if (processing.decrementAndGet() == 0 && !paused) {
            f.resume()
          }
        }
  }

  /**
   * Add a chunk to the store. Retry operation several times before failing.
   * @param chunk the chunk to add
   * @param meta the chunk's metadata
   * @param layer the layer the chunk should be added to (may be null)
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @return a Completable that will complete when the operation has finished
   */
  private fun addToStore(chunk: String, meta: ChunkMeta,
      layer: String, indexMeta: IndexMeta): Completable {
    return Completable.defer { store.rxAdd(chunk, meta, layer, indexMeta) }
        .retryWhen(RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log))
  }
}
