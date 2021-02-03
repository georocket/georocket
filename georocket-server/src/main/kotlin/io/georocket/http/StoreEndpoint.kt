package io.georocket.http

import io.georocket.ServerAPIException
import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.output.Merger
import io.georocket.output.MultiMerger
import io.georocket.storage.ChunkMeta
import io.georocket.storage.DeleteMeta
import io.georocket.storage.Store
import io.georocket.storage.StoreCursor
import io.georocket.storage.StoreFactory
import io.georocket.tasks.ReceivingTask
import io.georocket.tasks.TaskError
import io.georocket.util.HttpException
import io.georocket.util.MimeTypeUtils
import io.vertx.core.Vertx
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.core.file.deleteAwait
import io.vertx.kotlin.core.file.mkdirsAwait
import io.vertx.kotlin.core.file.openAwait
import io.vertx.kotlin.core.file.writeAwait
import io.vertx.kotlin.coroutines.toChannel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.text.StringEscapeUtils
import org.apache.http.ParseException
import org.apache.http.entity.ContentType
import org.bson.types.ObjectId
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.time.Instant
import java.util.regex.Pattern
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

  override fun createRouter(): Router {
    store = StoreFactory.createStore(vertx)
    storagePath = vertx.orCreateContext.config()
        .getString(ConfigConstants.STORAGE_FILE_PATH)

    val router = Router.router(vertx)
    router.get("/*").handler(this::onGet)
    router.put("/*").handler(this::onPut)
    router.post("/*").handler(this::onPost)
    router.delete("/*").handler(this::onDelete)

    return router
  }

  /**
   * Perform a search and merge all retrieved chunks using the given merger
   * @param merger the merger
   * @param data Data to merge into the response
   * @param out the response to write the merged chunks to
   * @param trailersAllowed `true` if the HTTP client accepts trailers
   * @return a single that will emit one item when all chunks have been merged
   */
  private suspend fun doMerge(merger: Merger<ChunkMeta>, data: StoreCursor,
      out: HttpServerResponse, trailersAllowed: Boolean) {
    var accepted = 0L
    var notaccepted = 0L

    for (chunk in data) {
      val crs = store.getOne(data.chunkPath)
      try {
        merger.merge(crs, chunk, out)
        accepted++
      } catch (e: IllegalStateException) {
        // Chunk cannot be merged. maybe it's a new one that has
        // been added after the merger was initialized. Just
        // ignore it, but emit a warning later
        notaccepted++
      } finally {
        // don't forget to close the chunk!
        crs.close()
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

    if (trailersAllowed) {
      out.putTrailer(TRAILER_UNMERGED_CHUNKS, notaccepted.toString())
    }

    if (accepted > 0) {
      merger.finish(out)
    } else {
      throw FileNotFoundException("Not Found")
    }
  }

  /**
   * Read the routing [context], select the right [StoreCursor] and set
   * response headers. For the first call to this method within a request,
   * [preview] must equal `true`, and for the second one (if there is one),
   * the parameter must equal `false`. This method must not be called more than
   * two times within a request.
   */
  private suspend fun prepareCursor(context: RoutingContext, preview: Boolean): StoreCursor {
    val request = context.request()
    val response = context.response()

    val scroll = request.getParam("scroll")
    val scrollIdParam = request.getParam("scrollId")
    val scrolling = BooleanUtils.toBoolean(scroll) || scrollIdParam != null

    // if we're generating a preview, split the scrollId param at ':' and
    // use the first part. Otherwise use the second one.
    val scrollId = if (scrollIdParam != null) {
      val scrollIdParts = scrollIdParam.split(":")
      if (preview) {
        scrollIdParts[0]
      } else {
        scrollIdParts[1]
      }
    } else {
      null
    }

    val path = Endpoint.getEndpointPath(context)
    val search = request.getParam("search")
    val strSize = request.getParam("size")
    val size = strSize?.toInt() ?: 100

    val cursor = if (scrolling) {
      if (scrollId == null) {
        store.scroll(search, path, size)
      } else {
        store.scroll(scrollId)
      }
    } else {
      store.get(search, path)
    }

    if (scrolling) {
      // create a new scrollId consisting of the one used for the preview and
      // the other one used for the real query
      var newScrollId = cursor.info.scrollId

      if (!preview) {
        var oldScrollId = response.headers()["X-Scroll-Id"]

        if (isOptimisticMerging(request)) {
          oldScrollId = "0"
        } else if (oldScrollId == null) {
          throw IllegalStateException("A preview must be generated " +
              "before the actual request can be made. This usually happens " +
              "when the merger is initialized.")
        }

        newScrollId = "$oldScrollId:$newScrollId"
      }

      response
          .putHeader("X-Scroll-Id", newScrollId)
          .putHeader("X-Total-Hits", cursor.info.totalHits.toString())
          .putHeader("X-Hits", cursor.info.currentHits.toString())
    }

    return cursor
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
    return te != null && te.toLowerCase().contains("trailers")
  }

  /**
   * Retrieve all chunks matching the specified query and path
   */
  private suspend fun getChunks(context: RoutingContext) {
    val request = context.request()
    val response = context.response()

    // Our responses must always be chunked because we cannot calculate
    // the exact content-length beforehand. We perform two searches, one to
    // initialize the merger and one to do the actual merge. The problem is
    // that the result set may change between these two searches and so we
    // cannot calculate the content-length just from looking at the result
    // from the first search.
    response.isChunked = true

    val optimisticMerging = isOptimisticMerging(request)
    val isTrailerAccepted = isTrailerAccepted(request)

    if (isTrailerAccepted) {
      response.putHeader("Trailer", TRAILER_UNMERGED_CHUNKS)
    }

    try {
      // perform two searches: first initialize the merger and then
      // merge all retrieved chunks
      val merger = MultiMerger(optimisticMerging)

      // skip initialization if optimistic merging is enabled
      if (!optimisticMerging) {
        val cursor = prepareCursor(context, true)
        while (cursor.hasNext()) {
          val cm = cursor.next()
          merger.init(cm)
        }
      }

      doMerge(merger, prepareCursor(context, false), response, isTrailerAccepted)
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
  private suspend fun getAttributeValues(search: String, path: String,
      attribute: String, response: HttpServerResponse) {
    var first = true
    response.isChunked = true
    response.write("[")

    val cursor = store.getAttributeValues(search, path, attribute)

    try {
      while (cursor.hasNext()) {
        val x = cursor.next()

        if (first) {
          first = false
        } else {
          response.write(",")
        }

        response.write(Json.mapper.writeValueAsString(x))
      }

      response
          .write("]")
          .setStatusCode(200)
          .end()
    } catch (t: Throwable) {
      Endpoint.fail(response, t)
    }
  }

  /**
   * Get the values of the specified [property] of all chunks matching the
   * given [search] query and [path]
   */
  private suspend fun getPropertyValues(search: String, path: String,
      property: String, response: HttpServerResponse) {
    var first = true
    response.isChunked = true
    response.write("[")

    try {
      val cursor = store.getPropertyValues(search, path, property)

      while (cursor.hasNext()) {
        val x = cursor.next()

        if (first) {
          first = false
        } else {
          response.write(",")
        }

        response.write("\"" + StringEscapeUtils.escapeJson(x) + "\"")
      }

      response
          .write("]")
          .setStatusCode(200)
          .end()
    } catch (t: Throwable) {
      Endpoint.fail(response, t)
    }
  }

  /**
   * Try to detect the content type of a file with the given [filepath].
   * Also consider if the file is [gzip] compressed or not.
   */
  private suspend fun detectContentType(filepath: String, gzip: Boolean): String {
    return vertx.executeBlockingAwait { f ->
      try {
        var mimeType = MimeTypeUtils.detect(File(filepath), gzip)
        if (mimeType == null) {
          log.warn("Could not detect file type of $filepath. Falling back to " +
              "application/octet-stream.")
          mimeType = "application/octet-stream"
        }
        f.complete(mimeType)
      } catch (e: IOException) {
        f.fail(e)
      }
    } ?: throw HttpException(215)
  }

  /**
   * Handles the HTTP POST request
   */
  private fun onPost(context: RoutingContext) {
    val request = context.request()
    val requestChannel = request.toChannel(vertx)

    val layer = Endpoint.getEndpointPath(context)
    val tagsStr = request.getParam("tags")
    val propertiesStr = request.getParam("props")
    val fallbackCRSString = request.getParam("fallbackCRS")

    val tags = if (StringUtils.isNotEmpty(tagsStr))
      tagsStr.split(",").map { it.trim() } else null

    val properties = mutableMapOf<String, Any>()
    if (StringUtils.isNotEmpty(propertiesStr)) {
      val regex = "(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":")
      val parts = propertiesStr.split(",").map { it.trim() }
      for (part in parts) {
        val property = part.split(regex)
        if (property.size != 2) {
          request.response()
              .setStatusCode(400)
              .end("Invalid property syntax: $part")
          return
        }
        val key = StringEscapeUtils.unescapeJava(property[0].trim())
        val value = StringEscapeUtils.unescapeJava(property[1].trim())
        properties[key] = value
      }
    }

    // get temporary filename
    val incoming = "$storagePath/incoming"
    val filename = ObjectId().toString()
    val filepath = "$incoming/$filename"

    val correlationId = ObjectId().toString()
    val startTime = System.currentTimeMillis()
    onReceivingFileStarted(correlationId)

    launch {
      // create directory for incoming files
      val fs = vertx.fileSystem()
      fs.mkdirsAwait(incoming)

      // create temporary file
      val f = fs.openAwait(filepath, OpenOptions())

      try {
        // write request body into temporary file
        for (buf in requestChannel) {
          f.writeAwait(buf)
        }
        f.close()

        val contentTypeHeader = request.getHeader("Content-Type")
        val mimeType = try {
          val contentType = ContentType.parse(contentTypeHeader)
          contentType.mimeType
        } catch (ex: ParseException) {
          null
        } catch (ex: IllegalArgumentException) {
          null
        }

        val contentEncoding = request.getHeader("Content-Encoding")
        val gzip = "gzip" == contentEncoding

        // detect content type of file to import
        val detectedContentType = if (mimeType == null || mimeType.isBlank() ||
            mimeType == "application/octet-stream" ||
            mimeType == "application/x-www-form-urlencoded") {
          // fallback: if the client has not sent a Content-Type or if it's
          // a generic one, then try to guess it
          log.debug("Mime type '$mimeType' is invalid or generic. "
              + "Trying to guess the right type.")
          detectContentType(filepath, gzip).also {
            log.info("Guessed mime type '$it'.")
          }
        } else {
          mimeType
        }

        val duration = System.currentTimeMillis() - startTime
        onReceivingFileFinished(correlationId, duration, null)

        // run importer
        val msg = JsonObject()
            .put("filename", filename)
            .put("layer", layer)
            .put("contentType", detectedContentType)
            .put("correlationId", correlationId)
            .put("contentEncoding", contentEncoding)

        if (tags != null) {
          msg.put("tags", JsonArray(tags))
        }

        if (properties.isNotEmpty()) {
          msg.put("properties", JsonObject(properties))
        }

        if (fallbackCRSString != null) {
          msg.put("fallbackCRSString", fallbackCRSString)
        }

        request.response()
            .setStatusCode(202) // Accepted
            .putHeader("X-Correlation-Id", correlationId)
            .setStatusMessage("Accepted file - importing in progress")
            .end()

        // run importer
        vertx.eventBus().send(AddressConstants.IMPORTER_IMPORT, msg)
      } catch (t: Throwable) {
        val duration = System.currentTimeMillis() - startTime
        onReceivingFileFinished(correlationId, duration, t)
        Endpoint.fail(request.response(), t)
        log.error(t)
        fs.deleteAwait(filepath)
      }
    }
  }

  private fun onReceivingFileStarted(correlationId: String) {
    log.info("Receiving file [$correlationId]")
    val task = ReceivingTask(correlationId)
    task.startTime = Instant.now()
    vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(task))
  }

  private fun onReceivingFileFinished(correlationId: String, duration: Long,
      error: Throwable?) {
    if (error == null) {
      log.info(String.format("Finished receiving file [%s] after %d ms",
          correlationId, duration))
    } else {
      log.error(String.format("Failed receiving file [%s] after %d ms",
          correlationId, duration), error)
    }
    val task = ReceivingTask(correlationId)
    task.endTime = Instant.now()
    if (error != null) {
      task.addError(TaskError(error))
    }
    vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(task))
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
        val strAsync = request.getParam("async")
        val async = BooleanUtils.toBoolean(strAsync)
        deleteChunks(search, path, async, response)
      }
    }
  }

  /**
   * Remove [properties] from all chunks matching the given [search] query
   * and [path]
   */
  private suspend fun removeProperties(search: String, path: String,
      properties: String, response: HttpServerResponse) {
    val list = properties.split(",")
    try {
      store.removeProperties(search, path, list)
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
  private suspend fun removeTags(search: String, path: String, tags: String,
      response: HttpServerResponse) {
    val list = tags.split(",")
    try {
      store.removeTags(search, path, list)
      response
          .setStatusCode(204)
          .end()
    } catch (t: Throwable) {
      Endpoint.fail(response, t)
    }
  }

  /**
   * Delete all chunks matching the given [search] query and [path]. The
   * [async] flag specifies if chunks should be deleted asynchronously or not.
   */
  private suspend fun deleteChunks(search: String, path: String, async: Boolean,
      response: HttpServerResponse) {
    val correlationId = ObjectId().toString()
    val deleteMeta = DeleteMeta(correlationId)
    if (async) {
      response
          .setStatusCode(202) // Accepted
          .putHeader("X-Correlation-Id", correlationId)
          .end()
      try {
        store.delete(search, path, deleteMeta)
      } catch (t: Throwable) {
        log.error("Could not delete chunks", t)
      }
    } else {
      try {
        store.delete(search, path, deleteMeta)
        response
            .setStatusCode(204) // No Content
            .putHeader("X-Correlation-Id", correlationId)
            .end()
      } catch (t: Throwable) {
        log.error("Could not delete chunks", t)
        Endpoint.fail(response, t)
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
    val search = request.getParam("search")
    val properties = request.getParam("properties")
    val tags = request.getParam("tags")

    launch {
      if (StringUtils.isNotEmpty(properties) || StringUtils.isNotEmpty(tags)) {
        try {
          if (StringUtils.isNotEmpty(properties)) {
            val parsedProperties = parseProperties(properties.split(","))
            store.setProperties(search, path, parsedProperties)
          }

          if (StringUtils.isNotEmpty(tags)) {
            store.appendTags(search, path, tags.split(","))
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
   * Parse list of [properties] in the form `key:value` to a map
   */
  private fun parseProperties(properties: List<String>): Map<String, String> {
    val props = mutableMapOf<String, String>()
    val regex = "(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":")

    for (part in properties.map { it.trim() }) {
      val property = part.split(regex)
      if (property.size != 2) {
        throw ServerAPIException(ServerAPIException.INVALID_PROPERTY_SYNTAX_ERROR,
            "Invalid property syntax: $part")
      }
      val key = StringEscapeUtils.unescapeJava(property[0].trim())
      val value = StringEscapeUtils.unescapeJava(property[1].trim())
      props[key] = value
    }

    return props
  }
}
