package io.georocket

import com.fasterxml.aalto.AsyncXMLStreamReader
import com.fasterxml.aalto.stax.InputFactoryImpl
import de.undercouch.actson.JsonEvent
import de.undercouch.actson.JsonParser
import io.georocket.constants.AddressConstants
import io.georocket.index.MainIndexer
import io.georocket.index.xml.XMLCRSIndexer
import io.georocket.input.geojson.GeoJsonSplitter
import io.georocket.input.xml.FirstLevelSplitter
import io.georocket.storage.IndexMeta
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.tasks.ImportingTask
import io.georocket.tasks.TaskError
import io.georocket.tasks.TaskRegistry
import io.georocket.util.JsonStreamEvent
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.StringWindow
import io.georocket.util.UTF8BomFilter
import io.georocket.util.Window
import io.georocket.util.XMLStreamEvent
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.streams.ReadStream
import io.vertx.kotlin.core.file.openOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.toReceiveChannel
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import org.apache.commons.io.FilenameUtils
import org.slf4j.LoggerFactory
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
  private lateinit var indexer: MainIndexer

  override suspend fun start() {
    log.info("Launching importer ...")

    store = StoreFactory.createStore(vertx)
    indexer = MainIndexer.create(coroutineContext, vertx)

    vertx.eventBus().localConsumer<JsonObject>(AddressConstants.IMPORTER_IMPORT) { msg ->
      launch {
        onImport(msg)
      }
    }
  }

  override suspend fun stop() {
    store.close()
    indexer.close()
  }

  /**
   * Receives a name of a file to import
   */
  private suspend fun onImport(msg: Message<JsonObject>) {
    val body = msg.body()
    val filepath = body.getString("filepath")
    val layer = body.getString("layer") ?: "/"
    val contentType = body.getString("contentType")
    val correlationId = body.getString("correlationId")
    val fallbackCRSString = body.getString("fallbackCRSString")
    val deleteOnFinish = body.getBoolean("deleteOnFinish", false)

    val filename = FilenameUtils.getName(filepath)

    // get tags
    val tags = body.getJsonArray("tags")
      ?.filterNotNull()
      ?.map { it.toString() }
      ?.toList()

    // get properties
    val properties = body.getJsonObject("properties")?.map

    // generate timestamp for this import
    val timestamp = System.currentTimeMillis()

    log.info("Importing [$correlationId] to layer '$layer'")

    val fs = vertx.fileSystem()
    val props = fs.props(filepath).await()
    val fileSize = props.size()
    val f = fs.open(filepath, openOptionsOf(create = false, write = false)).await()

    try {
      val chunkCount = importFile(f, fileSize, contentType, correlationId, filename,
          timestamp, layer, tags, properties, fallbackCRSString, msg)

      val duration = System.currentTimeMillis() - timestamp
      log.info("Finished importing [$correlationId] with $chunkCount" +
          " chunks to layer '$layer' after $duration ms")
    } catch (t: Throwable) {
      val duration = System.currentTimeMillis() - timestamp
      log.error("Failed to import [$correlationId] to layer '$layer'" +
          " after $duration ms", t)
    } finally {
      f.close().await()
      if (deleteOnFinish) {
        // delete file from 'incoming' folder
        log.debug("Deleting $filepath")
        try {
          fs.delete(filepath).await()
        } catch (t: Throwable) {
          log.error("Could not delete file $filepath", t)
        }
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
   * if the imported file does not specify one.
   */
  private suspend fun importFile(file: ReadStream<Buffer>, fileSize: Long, contentType: String,
      correlationId: String, filename: String, timestamp: Long, layer: String,
      tags: List<String>?, properties: Map<String, Any>?, fallbackCRSString: String?,
      msg: Message<JsonObject>): Long {
    // let the task verticle know that we're now importing
    var importingTask = ImportingTask(correlationId = correlationId, bytesTotal = fileSize)
    TaskRegistry.upsert(importingTask)

    // let sender know that we are now going to import the file and the progress
    // can be monitored with the given task ID
    msg.reply(importingTask.id)

    var lastChunksAdded = 0L
    val progressUpdater = { chunksAdded: Long, bytesProcessed: Long, final: Boolean ->
      if (chunksAdded - lastChunksAdded >= 100 || final) {
        importingTask = importingTask.copy(importedChunks = chunksAdded,
          bytesProcessed = bytesProcessed)
        TaskRegistry.upsert(importingTask)
        lastChunksAdded = chunksAdded
      }
    }

    try {
      val result = if (belongsTo(contentType, "application", "xml") ||
          belongsTo(contentType, "text", "xml")) {
        importXML(file, correlationId, filename, timestamp, layer, tags,
            properties, fallbackCRSString, progressUpdater)
      } else if (belongsTo(contentType, "application", "json")) {
        importJSON(file, correlationId, filename, timestamp, layer, tags,
          properties, progressUpdater)
      } else {
        throw IllegalArgumentException("Received an unexpected content " +
            "type '$contentType' while trying to import file '$filename'")
      }

      importingTask = importingTask.copy(endTime = Instant.now())
      TaskRegistry.upsert(importingTask)

      return result
    } catch (t: Throwable) {
      importingTask = importingTask.copy(endTime = Instant.now(),
        error = TaskError(t))
      TaskRegistry.upsert(importingTask)
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
      properties: Map<String, Any>?, fallbackCRSString: String?,
      updateProgress: (Long, Long, Boolean) -> Unit): Long {
    var chunksAdded = 0L
    var bytesProcessed = 0L

    val bomFilter = UTF8BomFilter()
    val window = Window()
    val splitter = FirstLevelSplitter(window)
    val crsIndexer = XMLCRSIndexer()
    val parser = InputFactoryImpl().createAsyncForByteArray()

    val makeIndexMeta = { crsString: String? ->
      IndexMeta(correlationId, filename, timestamp, tags, properties, crsString)
    }

    var indexMeta = makeIndexMeta(fallbackCRSString)

    var flushQueueJob: Deferred<Unit>? = null
    val batchSize = 200 // TODO make configurable
    val queue = mutableListOf<Pair<Buffer, String>>()
    suspend fun flushQueue() {
      if (queue.isEmpty()) {
        return
      }
      val start = System.currentTimeMillis()
      store.addMany(queue)
      log.info("Finished importing $batchSize chunks in ${System.currentTimeMillis() - start} ms")
      queue.clear()
    }

    val processEvents: suspend () -> Boolean = pe@{
      while (true) {
        val nextEvent = parser.next()
        if (nextEvent == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
          break
        }

        // create stream event
        val pos = parser.location.characterOffset
        val streamEvent = XMLStreamEvent(nextEvent, pos, parser)

        // save the first CRS found in the file
        if (crsIndexer.crs == null) {
          crsIndexer.onEvent(streamEvent)
          if (crsIndexer.crs != null) {
            indexMeta = makeIndexMeta(crsIndexer.crs)
          }
        }

        val result = splitter.onEvent(streamEvent)
        if (result != null) {
          val path = store.makePath(indexMeta, layer)
          queue.add(result.chunk to path)
          if (queue.size >= batchSize) {
            flushQueueJob?.await()
            flushQueueJob = async {
              flushQueue()
            }
          }
          indexer.add(result.chunk, result.meta, indexMeta, path)
          chunksAdded++
          updateProgress(chunksAdded, bytesProcessed, false)
        }

        if (nextEvent == AsyncXMLStreamReader.END_DOCUMENT) {
          parser.close()
          return@pe false
        }
      }
      true
    }

    val channel = f.toReceiveChannel(vertx)
    for (bytebuf in channel) {
      val buf = bomFilter.filter(bytebuf)
      window.append(buf)

      val bytes = buf.bytes
      var i = 0
      while (i < bytes.size) {
        val len = bytes.size - i
        parser.inputFeeder.feedInput(bytes, i, len)
        i += len
        if (!processEvents()) {
          break
        }
      }

      bytesProcessed += buf.length()
    }

    // process remaining events
    parser.inputFeeder.endOfInput()
    processEvents()

    // wait for remaining chunks to be imported and indexed
    flushQueueJob?.await()
    flushQueue()
    indexer.flushAdd()

    updateProgress(chunksAdded, bytesProcessed, true)

    return chunksAdded
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
      properties: Map<String, Any>?, updateProgress: (Long, Long, Boolean) -> Unit): Long {
    var chunksAdded = 0L
    var bytesProcessed = 0L

    val bomFilter = UTF8BomFilter()
    val window = StringWindow()
    val splitter = GeoJsonSplitter(window)
    val parser = JsonParser()

    val indexMeta = IndexMeta(correlationId, filename, timestamp, tags,
        properties, null)

    var flushQueueJob: Deferred<Unit>? = null
    val batchSize = 200 // TODO make configurable
    val queue = mutableListOf<Pair<Buffer, String>>()
    suspend fun flushQueue() {
      if (queue.isEmpty()) {
        return
      }
      val start = System.currentTimeMillis()
      store.addMany(queue)
      log.info("Finished importing $batchSize chunks in ${System.currentTimeMillis() - start} ms")
      queue.clear()
    }

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
          val path = store.makePath(indexMeta, layer)
          queue.add(result.chunk to path)
          if (queue.size >= batchSize) {
            flushQueueJob?.await()
            flushQueueJob = async {
              flushQueue()
            }
          }
          indexer.add(result.chunk, result.meta, indexMeta, path)
          chunksAdded++
          updateProgress(chunksAdded, bytesProcessed, false)
        }

        if (nextEvent == JsonEvent.EOF) {
          return@pe false
        }
      }
      true
    }

    val channel = f.toReceiveChannel(vertx)
    for (bytebuf in channel) {
      val buf = bomFilter.filter(bytebuf)
      window.append(buf)

      val bytes = buf.bytes
      var i = 0
      while (i < bytes.size) {
        i += parser.feeder.feed(bytes, i, bytes.size - i)
        if (!processEvents()) {
          break
        }
      }

      bytesProcessed += buf.length()
    }

    // process remaining events
    parser.feeder.done()
    processEvents()

    // wait for remaining chunks to be imported and indexed
    flushQueueJob?.await()
    flushQueue()
    indexer.flushAdd()

    updateProgress(chunksAdded, bytesProcessed, true)

    return chunksAdded
  }
}
