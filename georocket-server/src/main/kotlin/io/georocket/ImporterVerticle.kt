package io.georocket

import de.undercouch.actson.JsonEvent
import de.undercouch.actson.JsonParser
import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.input.geojson.GeoJsonSplitter
import io.georocket.storage.IndexMeta
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.tasks.ImportingTask
import io.georocket.tasks.TaskError
import io.georocket.util.JsonStreamEvent
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.StringWindow
import io.georocket.util.UTF8BomFilter
import io.georocket.util.io.GzipReadStream
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.file.AsyncFile
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.core.streams.ReadStream
import io.vertx.kotlin.core.file.closeAwait
import io.vertx.kotlin.core.file.deleteAwait
import io.vertx.kotlin.core.file.openAwait
import io.vertx.kotlin.core.file.openOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.toChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.map
import kotlinx.coroutines.launch
import java.time.Instant


/**
 * Imports file in the background
 * @author Michel Kraemer
 */
class ImporterVerticle : CoroutineVerticle() {
  companion object {
    private val log = LoggerFactory.getLogger(ImporterVerticle::class.java)
  }

  private lateinit var store: Store
  private lateinit var incoming: String
  private var paused = false
  private val filesBeingImported = mutableSetOf<AsyncFile>()

  override suspend fun start() {
    log.info("Launching importer ...")

    store = StoreFactory.createStore(vertx)
    val storagePath = config.getString(ConfigConstants.STORAGE_FILE_PATH)
    incoming = "$storagePath/incoming"

    vertx.eventBus().localConsumer<JsonObject>(AddressConstants.IMPORTER_IMPORT) { msg ->
      launch {
        onImport(msg)
      }
    }
    vertx.eventBus().localConsumer(AddressConstants.IMPORTER_PAUSE, this::onPause)
  }

  /**
   * Receives a name of a file to import
   */
  private suspend fun onImport(msg: Message<JsonObject>) {
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
    val f = fs.openAwait(filepath, openOptionsOf(create = false, write = false))
    filesBeingImported.add(f)

    try {
      val chunkCount = importFile(f, contentType, correlationId, filename,
          timestamp, layer, tags, properties, fallbackCRSString, contentEncoding)

      val duration = System.currentTimeMillis() - timestamp
      log.info("Finished importing [$correlationId] with $chunkCount" +
          " chunks to layer '$layer' after $duration ms")
    } catch (t: Throwable) {
      val duration = System.currentTimeMillis() - timestamp
      log.error("Failed to import [$correlationId] to layer '$layer'" +
          " after $duration ms", t)
    } finally {
      filesBeingImported.remove(f)

      // delete file from 'incoming' folder
      log.debug("Deleting $filepath from incoming folder")
      f.closeAwait()
      try {
        fs.deleteAwait(filepath)
      } catch (t: Throwable) {
        log.error("Could not delete file from 'incoming' folder", t)
      }
    }
  }

  /**
   * Import a [file] from the given read stream into the store. Inspect the
   * file's [contentType] and forward to the correct import method. Callers
   * should provide a [correlationId] so the import progress can be tracked
   * correctly. The method will also attach metadata to the imported chunks
   * such as the original [filename], the [timestamp] when the import was
   * started, the [layer] where chunks will be stored, as well as optional
   * [tags] and [properties]. If necessary, a [fallbackCRSString] can be given
   * if the imported file does not specify one. The file will be read as a raw
   * stream or using the given [contentEncoding] if provided.
   */
  private suspend fun importFile(file: ReadStream<Buffer>, contentType: String,
      correlationId: String, filename: String, timestamp: Long, layer: String,
      tags: List<String>?, properties: Map<String, Any>?, fallbackCRSString: String?,
      contentEncoding: String?): Int {
    val f = if ("gzip" == contentEncoding) {
      log.debug("Importing file compressed with GZIP")
      GzipReadStream(file)
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

    try {
      val result = if (belongsTo(contentType, "application", "xml") ||
          belongsTo(contentType, "text", "xml")) {
        importXML(f, correlationId, filename, timestamp, layer, tags,
            properties, fallbackCRSString)
      } else if (belongsTo(contentType, "application", "json")) {
        importJSON(f, correlationId, filename, timestamp, layer, tags, properties)
      } else {
        throw IllegalArgumentException("Received an unexpected content " +
            "type '$contentType' while trying to import file '$filename'")
      }

      // let the task verticle know that we imported n chunks
      // TODO this should happen WHILE we are importing (e.g. every 100 chunks)
      val currentTask = ImportingTask(correlationId)
      currentTask.importedChunks = result.toLong()
      vertx.eventBus().publish(AddressConstants.TASK_INC,
          JsonObject.mapFrom(currentTask))

      // let the task verticle know that the import process has finished
      val endTask = ImportingTask(correlationId)
      endTask.endTime = Instant.now()
      vertx.eventBus().publish(AddressConstants.TASK_INC,
          JsonObject.mapFrom(endTask))

      return result
    } catch (t: Throwable) {
      val endTask = ImportingTask(correlationId)
      endTask.endTime = Instant.now()
      endTask.addError(TaskError(t))
      vertx.eventBus().publish(AddressConstants.TASK_INC,
          JsonObject.mapFrom(endTask))
      throw t
    }
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
  private suspend fun importXML(f: ReadStream<Buffer>, correlationId: String,
      filename: String, timestamp: Long, layer: String, tags: List<String>?,
      properties: Map<String, Any>?, fallbackCRSString: String?): Int {
    /*val bomFilter = UTF8BomFilter()
    val window = Window()
    val splitter: XMLSplitter = FirstLevelSplitter(window)
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
          store.add(result.chunk, result.meta, indexMeta, layer)
        }, false, MAX_PARALLEL_ADDS)*/
    TODO()
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
  private suspend fun importJSON(f: ReadStream<Buffer>, correlationId: String,
      filename: String, timestamp: Long, layer: String, tags: List<String>?,
      properties: Map<String, Any>?): Int {
    var chunksAdded = 0

    val bomFilter = UTF8BomFilter()
    val window = StringWindow()
    val splitter = GeoJsonSplitter(window)
    val parser = JsonParser()

    val indexMeta = IndexMeta(correlationId, filename, timestamp, tags,
        properties, null)

    val processEvents: suspend () -> Boolean = pe@{
      while (true) {
        val nextEvent = parser.nextEvent()
        val value = when (nextEvent) {
          JsonEvent.NEED_MORE_INPUT -> break
          JsonEvent.ERROR -> throw IllegalStateException("Syntax error")
          JsonEvent.VALUE_STRING, JsonEvent.FIELD_NAME -> parser.currentString
          JsonEvent.VALUE_DOUBLE -> parser.currentDouble
          JsonEvent.VALUE_INT -> parser.currentInt
          else -> null
        }

        val streamEvent = JsonStreamEvent(nextEvent, parser.parsedCharacterCount, value)
        val result = splitter.onEvent(streamEvent)
        if (result != null) {
          store.add(result.chunk, result.meta, indexMeta, layer)
          chunksAdded++
        }

        if (nextEvent == JsonEvent.EOF) {
          return@pe false
        }
      }
      true
    }

    val channel = f.toChannel(vertx)
    channel.map { bomFilter.filter(it) }.consumeEach { buf ->
      log.info(buf.toString())

      window.append(buf)

      val bytes = buf.bytes
      var i = 0
      while (i < bytes.size) {
        i += parser.feeder.feed(bytes, i, bytes.size - i)
        if (!processEvents()) {
          break
        }
      }
    }

    // process remaining events
    parser.feeder.done()
    processEvents()

    return chunksAdded
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
}
