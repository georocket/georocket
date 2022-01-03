package io.georocket.http

import io.georocket.ServerAPIException
import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.index.Index
import io.georocket.index.IndexFactory
import io.georocket.index.IndexerFactory
import io.georocket.index.MetaIndexerFactory
import io.georocket.index.PropertiesParser
import io.georocket.index.TagsParser
import io.georocket.output.MultiMerger
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.tasks.ReceivingTask
import io.georocket.tasks.TaskError
import io.georocket.tasks.TaskRegistry
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.MimeTypeUtils.detect
import io.georocket.util.collectChunked
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.toReceiveChannel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.launch
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.StringUtils
import org.apache.http.ParseException
import org.apache.http.entity.ContentType
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.time.Instant
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

/**
 * An HTTP endpoint handling requests related to the GeoRocket data store
 * @author Michel Kraemer
 */
class StoreEndpoint(override val coroutineContext: CoroutineContext,
    private val vertx: Vertx) : Endpoint, CoroutineScope {
  companion object {
    private val log = LoggerFactory.getLogger(StoreEndpoint::class.java)

    /**
     * Name of the HTTP trailer that tells the client how many chunks could not
     * be merged. Possible reasons for unmerged chunks are:
     *
     *  * New chunks were added to the store while merging was in progress.
     *  * Optimistic merging was enabled and some chunks could not be merged.
     *
     * The trailer will contain the number of chunks that could not be
     * merged. The client can decide whether to repeat the request to fetch
     * the missing chunks (e.g. with optimistic merging disabled) or not.
     */
    private const val TRAILER_UNMERGED_CHUNKS = "GeoRocket-Unmerged-Chunks"
  }

  private lateinit var store: Store
  private lateinit var storagePath: String

  /**
   * The GeoRocket index
   */
  private lateinit var index: Index

  override suspend fun createRouter(): Router {
    store = StoreFactory.createStore(vertx)
    storagePath = vertx.orCreateContext.config()
        .getString(ConfigConstants.STORAGE_FILE_PATH)

    index = IndexFactory.createIndex(vertx)

    val router = Router.router(vertx)
    router.get("/*").handler(this::onGet)
    router.put("/*").handler(this::onPut)
    router.post("/*").handler(this::onPost)
    router.delete("/*").handler(this::onDelete)

    return router
  }

  override suspend fun close() {
    index.close()
    store.close()
  }

  /**
   * Handles the HTTP GET request for a bunch of chunks
   */
  private fun onGet(context: RoutingContext) {
    val request = context.request()
    val response = context.response()

    val path = Endpoint.getEndpointPath(context)
    val search = request.getParam("search")
    val property = request.getParam("property")
    val attribute = request.getParam("attribute")

    launch {
      if (property != null && attribute != null) {
        response
            .setStatusCode(400)
            .end("You can only get the values of a property or an attribute, but not both")
      } else if (property != null) {
        getPropertyValues(search, path, property, response)
      } else if (attribute != null) {
        getAttributeValues(search, path, attribute, response)
      } else {
        getChunks(context)
      }
    }
  }

  /**
   * Checks if optimistic merging is enabled
   */
  private fun isOptimisticMerging(request: HttpServerRequest): Boolean {
    return BooleanUtils.toBoolean(request.getParam("optimisticMerging"))
  }

  /**
   * Checks if the client accepts an HTTP trailer
   */
  private fun isTrailerAccepted(request: HttpServerRequest): Boolean {
    val te = request.getHeader("TE")
    return te != null && te.lowercase(Locale.getDefault()).contains("trailers")
  }

  private fun compileQuery(search: String?, path: String): JsonObject {
    return DefaultQueryCompiler(MetaIndexerFactory.ALL + IndexerFactory.ALL)
      .compileQuery(search ?: "", path)
  }

  /**
   * Retrieve all chunks matching the specified query and path
   */
  private suspend fun getChunks(context: RoutingContext) {
    val request = context.request()
    val response = context.response()

    val path = Endpoint.getEndpointPath(context)
    val search = request.getParam("search")

    response.isChunked = true

    val optimisticMerging = isOptimisticMerging(request)
    val isTrailerAccepted = isTrailerAccepted(request)

    if (isTrailerAccepted) {
      response.putHeader("Trailer", TRAILER_UNMERGED_CHUNKS)
    }

    try {
      val query = compileQuery(search, path)

      val merger = MultiMerger(optimisticMerging)

      // skip initialization if optimistic merging is enabled
      if (!optimisticMerging) {
        val distinctMetas = index.getDistinctMeta(query)
        distinctMetas.collect { merger.init(it) }
      }

      // merge chunks
      var accepted = 0L
      var notaccepted = 0L
      val metas = index.getMeta(query)
      metas.collect { chunkMeta ->
        val chunk = store.getOne(chunkMeta.first)
        try {
          merger.merge(chunk, chunkMeta.second, response)
          accepted++
        } catch (e: IllegalStateException) {
          // Chunk cannot be merged. maybe it's a new one that has
          // been added after the merger was initialized. Just
          // ignore it, but emit a warning later
          notaccepted++
        }
      }

      if (notaccepted > 0) {
        log.warn("Could not merge " + notaccepted + " chunks "
            + "because the merger did not accept them. Most likely "
            + "these are new chunks that were added while "
            + "merging was in progress or those that were ignored "
            + "during optimistic merging. If this worries you, "
            + "just repeat the request.")
      }

      if (isTrailerAccepted) {
        response.putTrailer(TRAILER_UNMERGED_CHUNKS, notaccepted.toString())
      }

      if (accepted > 0) {
        merger.finish(response)
      } else {
        throw FileNotFoundException("Not Found")
      }

      response.end()
    } catch (t: Throwable) {
      if (t !is FileNotFoundException) {
        log.error("Could not perform query", t)
      }
      Endpoint.fail(response, t)
    }
  }

  /**
   * Get the values of the specified [attribute] of all chunks matching the
   * given [search] query and [path]
   */
  private suspend fun getAttributeValues(search: String?, path: String,
      attribute: String, response: HttpServerResponse) {
    var first = true
    response.isChunked = true
    response.setStatusCode(200).write("[")

    val query = compileQuery(search, path)
    val values = index.getAttributeValues(query, attribute)

    try {
      values.collect { v ->
        if (first) {
          first = false
        } else {
          response.write(",")
        }

        response.write(DatabindCodec.mapper().writeValueAsString(v))
      }

      response.write("]")
      response.end()
    } catch (t: Throwable) {
      log.error("Could not fetch attribute values", t)
      Endpoint.fail(response, t)
    }
  }

  /**
   * Get the values of the specified [property] of all chunks matching the
   * given [search] query and [path]
   */
  private suspend fun getPropertyValues(search: String?, path: String,
      property: String, response: HttpServerResponse) {
    var first = true
    response.isChunked = true
    response.setStatusCode(200).write("[")

    try {
      val query = compileQuery(search, path)
      val values = index.getPropertyValues(query, property)

      values.collect { v ->
        if (first) {
          first = false
        } else {
          response.write(",")
        }

        response.write(DatabindCodec.mapper().writeValueAsString(v))
      }

      response.write("]")
      response.end()
    } catch (t: Throwable) {
      log.error("Could not fetch property values", t)
      Endpoint.fail(response, t)
    }
  }

  /**
   * Try to detect the content type of a file with the given [filepath].
   */
  private suspend fun detectContentType(filepath: String): String {
    return vertx.executeBlocking<String> { f ->
      try {
        var mimeType = detect(File(filepath))
        if (mimeType == null) {
          log.warn("Could not detect file type of $filepath. Falling back to " +
              "application/octet-stream.")
          mimeType = "application/octet-stream"
        }
        f.complete(mimeType)
      } catch (e: IOException) {
        f.fail(e)
      }
    }.await()
  }

  /**
   * Handles the HTTP POST request
   */
  private fun onPost(context: RoutingContext) {
    val request = context.request()
    val requestChannel = request.toReceiveChannel(vertx)

    val layer = Endpoint.getEndpointPath(context)
    val tagsStr = request.getParam("tags")
    val propertiesStr = request.getParam("properties")
    val fallbackCRSString = request.getParam("fallbackCRS")

    val tags = if (StringUtils.isNotEmpty(tagsStr)) TagsParser.parse(tagsStr) else null

    val properties = try {
      parseProperties(propertiesStr)
    } catch (t: Throwable) {
      Endpoint.fail(context.response(), t)
      return
    }

    // get temporary filename
    val incoming = "$storagePath/incoming"
    val filename = ObjectId().toString()
    val filepath = "$incoming/$filename"

    val correlationId = ObjectId().toString()
    val startTime = System.currentTimeMillis()

    log.info("Receiving file [$correlationId]")
    val receivingTask = ReceivingTask(correlationId = correlationId)
    TaskRegistry.upsert(receivingTask)

    launch {
      // create directory for incoming files
      val fs = vertx.fileSystem()
      fs.mkdirs(incoming).await()

      // create temporary file
      val f = fs.open(filepath, OpenOptions()).await()

      try {
        // write request body into temporary file
        for (buf in requestChannel) {
          if (f.writeQueueFull()) {
            val p = Promise.promise<Unit>()
            f.drainHandler { p.complete() }
            p.future().await()
            f.drainHandler(null)
          }
          f.write(buf)
        }
        f.close().await()

        val contentTypeHeader = request.getHeader("Content-Type")
        val mimeType = try {
          val contentType = ContentType.parse(contentTypeHeader)
          contentType.mimeType
        } catch (ex: ParseException) {
          null
        } catch (ex: IllegalArgumentException) {
          null
        }

        // detect content type of file to import
        val detectedContentType = if (mimeType == null || mimeType.isBlank() ||
          mimeType == "application/octet-stream" ||
          mimeType == "application/x-www-form-urlencoded") {
          // fallback: if the client has not sent a Content-Type or if it's
          // a generic one, then try to guess it
          log.debug("Mime type '$mimeType' is invalid or generic. "
              + "Trying to guess the right type.")
          detectContentType(filepath).also {
            log.info("Guessed mime type '$it'.")
          }
        } else {
          mimeType
        }

        if (!belongsTo(detectedContentType, "application", "xml") &&
          !belongsTo(detectedContentType, "text", "xml") &&
          !belongsTo(detectedContentType, "application", "json")) {
          request.response()
            .setStatusCode(415)
            .end("Unsupported content type: $mimeType")
          return@launch
        }

        val duration = System.currentTimeMillis() - startTime
        onReceivingFileFinished(receivingTask, duration, null)

        // run importer
        val msg = JsonObject()
            .put("filepath", filepath)
            .put("layer", layer)
            .put("contentType", detectedContentType)
            .put("correlationId", correlationId)
            .put("deleteOnFinish", true) // delete file from 'incoming' folder after import

        if (tags != null) {
          msg.put("tags", JsonArray(tags))
        }

        if (properties.isNotEmpty()) {
          msg.put("properties", JsonObject(properties))
        }

        if (fallbackCRSString != null) {
          msg.put("fallbackCRSString", fallbackCRSString)
        }

        // run importer
        vertx.eventBus().send(AddressConstants.IMPORTER_IMPORT, msg)

        request.response()
            .setStatusCode(202) // Accepted
            .putHeader("X-Correlation-Id", correlationId)
            .setStatusMessage("Accepted file - importing in progress")
            .end()
      } catch (t: Throwable) {
        val duration = System.currentTimeMillis() - startTime
        onReceivingFileFinished(receivingTask, duration, t)
        Endpoint.fail(request.response(), t)
        log.error("Could not import file", t)
        fs.delete(filepath).await()
      }
    }
  }

  private fun onReceivingFileFinished(receivingTask: ReceivingTask,
      duration: Long, error: Throwable?) {
    if (error == null) {
      log.info(String.format("Finished receiving file [%s] after %d ms",
        receivingTask.correlationId, duration))
    } else {
      log.error(String.format("Failed receiving file [%s] after %d ms",
        receivingTask.correlationId, duration), error)
    }
    val updatedTask = if (error != null) {
      receivingTask.copy(endTime = Instant.now(), error = TaskError(error))
    } else {
      receivingTask.copy(endTime = Instant.now())
    }
    TaskRegistry.upsert(updatedTask)
  }

  /**
   * Handles the HTTP DELETE request
   * @param context the routing context
   */
  private fun onDelete(context: RoutingContext) {
    val path = Endpoint.getEndpointPath(context)
    val response = context.response()
    val request = context.request()
    val search = request.getParam("search")
    val properties = request.getParam("properties")
    val tags = request.getParam("tags")
    val await = BooleanUtils.toBoolean(request.getParam("await"))

    launch {
      if (StringUtils.isNotEmpty(properties) && StringUtils.isNotEmpty(tags)) {
        response
            .setStatusCode(400)
            .end("You can only delete properties or tags, but not both")
      } else if (StringUtils.isNotEmpty(properties)) {
        removeProperties(search, path, properties, response)
      } else if (StringUtils.isNotEmpty(tags)) {
        removeTags(search, path, tags, response)
      } else {
        deleteChunks(search, path, response, await)
      }
    }
  }

  /**
   * Remove [properties] from all chunks matching the given [search] query
   * and [path]
   */
  private suspend fun removeProperties(search: String?, path: String,
      properties: String, response: HttpServerResponse) {
    val list = TagsParser.parse(properties)
    try {
      val query = compileQuery(search, path)
      index.removeProperties(query, list)
      response
          .setStatusCode(204)
          .end()
    } catch (t: Throwable) {
      Endpoint.fail(response, t)
    }
  }

  /**
   * Remove [tags] from all chunks matching the given [search] query and [path]
   */
  private suspend fun removeTags(search: String?, path: String, tags: String,
      response: HttpServerResponse) {
    val list = TagsParser.parse(tags)
    try {
      val query = compileQuery(search, path)
      index.removeTags(query, list)
      response
          .setStatusCode(204)
          .end()
    } catch (t: Throwable) {
      Endpoint.fail(response, t)
    }
  }

  /**
   * Delete all chunks matching the given [search] query and [path].
   */
  private suspend fun deleteChunks(search: String?, path: String,
      response: HttpServerResponse, await: Boolean) {
    val answerSent = AtomicBoolean(false)
    coroutineScope {
      launch {
        try {
          val query = compileQuery(search, path)
          val paths = index.getPaths(query)
          var deleted = 0L
          paths.buffer().collectChunked(10000) { chunk ->
            index.delete(chunk)
            deleted += store.delete(chunk)
          }
          if (!answerSent.getAndSet(true)) {
            // All chunks have been deleted within the given timeframe
            response
              .setStatusCode(204) // No Content
              .end()
          }
          log.info("Successfully deleted $deleted chunks.")
        } catch (t: Throwable) {
          log.error("Could not delete chunks", t)
          if (!answerSent.getAndSet(true)) {
            Endpoint.fail(response, t)
          }
        }
      }

      if (!await) {
        launch {
          delay(1000)
          if (!answerSent.getAndSet(true)) {
            // Deletion took too long. Continue in the background ...
            response
              .setStatusCode(202) // Accepted
              .end()
          }
        }
      }
    }
  }

  /**
   * Handles the HTTP PUT request
   * @param context the routing context
   */
  private fun onPut(context: RoutingContext) {
    val path = Endpoint.getEndpointPath(context)
    val response = context.response()
    val request = context.request()
    val search = request.getParam("search") ?: ""
    val properties = request.getParam("properties")
    val tags = request.getParam("tags")

    launch {
      if (StringUtils.isNotEmpty(properties) || StringUtils.isNotEmpty(tags)) {
        val query = compileQuery(search, path)

        try {
          if (StringUtils.isNotEmpty(properties)) {
            val parsedProperties = parseProperties(properties)
            if (parsedProperties.isNotEmpty()) {
              index.setProperties(query, parsedProperties)
            }
          }

          if (StringUtils.isNotEmpty(tags)) {
            index.addTags(query, TagsParser.parse(tags))
          }

          response
              .setStatusCode(204)
              .end()
        } catch (t: Throwable) {
          Endpoint.fail(response, t)
        }
      } else {
        response
            .setStatusCode(405)
            .end("Only properties and tags can be modified")
      }
    }
  }

  /**
   * Parse list of [properties] in the form `key:value[,key:value,...]` to a map
   */
  private fun parseProperties(properties: String?): Map<String, Any> {
    if (properties == null) {
      return emptyMap()
    }

    return try {
      PropertiesParser.parse(properties)
    } catch (t: ParseCancellationException) {
      throw ServerAPIException(ServerAPIException.INVALID_PROPERTY_SYNTAX_ERROR,
        "Invalid property syntax: ${t.message}")
    }
  }
}
