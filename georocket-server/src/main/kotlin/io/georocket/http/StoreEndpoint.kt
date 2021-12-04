package io.georocket.http

import io.georocket.ServerAPIException
import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.index.Index
import io.georocket.index.IndexerFactory
import io.georocket.index.MetaIndexerFactory
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.output.MultiMerger
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.ChunkMeta
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.storage.XMLChunkMeta
import io.georocket.tasks.ReceivingTask
import io.georocket.tasks.TaskError
import io.georocket.util.FilteredServiceLoader
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
import java.util.Locale
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

  /**
   * The GeoRocket index
   */
  private lateinit var index: Index

  /**
   * A list of [MetaIndexerFactory] objects
   */
  private lateinit var metaIndexerFactories: List<MetaIndexerFactory>

  /**
   * A list of [IndexerFactory] objects
   */
  private lateinit var indexerFactories: List<IndexerFactory>

  override suspend fun createRouter(): Router {
    store = StoreFactory.createStore(vertx)
    storagePath = vertx.orCreateContext.config()
        .getString(ConfigConstants.STORAGE_FILE_PATH)

    index = MongoDBIndex.create(vertx)

    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    metaIndexerFactories = FilteredServiceLoader.load(MetaIndexerFactory::class.java).toList()
    indexerFactories = FilteredServiceLoader.load(IndexerFactory::class.java).toList()

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
    return DefaultQueryCompiler(metaIndexerFactories + indexerFactories)
      .compileQuery(search ?: "", path)
  }

  /**
   * Extract a path string and a [ChunkMeta] object from a given [hit] object
   */
  private fun createChunkMeta(hit: JsonObject): Pair<String, ChunkMeta> {
    val path = hit.getString("id")
    val mimeType = hit.getString("mimeType", XMLChunkMeta.MIME_TYPE)
    return path to if (MimeTypeUtils.belongsTo(mimeType, "application", "xml") ||
      MimeTypeUtils.belongsTo(mimeType, "text", "xml")) {
      XMLChunkMeta(hit)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "geo+json")) {
      GeoJsonChunkMeta(hit)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "json")) {
      JsonChunkMeta(hit)
    } else {
      ChunkMeta(hit)
    }
  }

  /**
   * Retrieve all chunks matching the specified query and path
   */
  private suspend fun getChunks(context: RoutingContext) {
    val request = context.request()
    val response = context.response()

    val path = Endpoint.getEndpointPath(context)
    val search = request.getParam("search")

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
      val query = compileQuery(search, path)
      val metas = index.getMeta(query).map { createChunkMeta(it) }

      val merger = MultiMerger(optimisticMerging)

      // skip initialization if optimistic merging is enabled
      if (!optimisticMerging) {
        metas.forEach { merger.init(it.second) }
      }

      // merge chunks
      var accepted = 0L
      var notaccepted = 0L

      for (chunkMeta in metas) {
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
      for (v in values) {
        if (first) {
          first = false
        } else {
          response.write(",")
        }

        response.write(Json.mapper.writeValueAsString(v))
      }

      response.write("]").end()
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

      for (v in values) {
        if (first) {
          first = false
        } else {
          response.write(",")
        }

        response.write(Json.mapper.writeValueAsString(v))
      }

      response.write("]").end()
    } catch (t: Throwable) {
      log.error("Could not fetch property values", t)
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
    val propertiesStr = request.getParam("properties")
    val fallbackCRSString = request.getParam("fallbackCRS")

    val tags = if (StringUtils.isNotEmpty(tagsStr))
      tagsStr.split(",").map { it.trim() } else null

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

        // run importer
        vertx.eventBus().send(AddressConstants.IMPORTER_IMPORT, msg)

        request.response()
            .setStatusCode(202) // Accepted
            .putHeader("X-Correlation-Id", correlationId)
            .setStatusMessage("Accepted file - importing in progress")
            .end()
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
        deleteChunks(search, path, response)
      }
    }
  }

  /**
   * Remove [properties] from all chunks matching the given [search] query
   * and [path]
   */
  private suspend fun removeProperties(search: String?, path: String,
      properties: String, response: HttpServerResponse) {
    val list = properties.split(",")
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
    val list = tags.split(",")
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
      response: HttpServerResponse) {
    try {
      val query = compileQuery(search, path)
      val hits = index.getMeta(query)
      index.delete(query)
      store.delete(hits.map { it.getString("id") })
      response
          .setStatusCode(204) // No Content
          .end()
    } catch (t: Throwable) {
      log.error("Could not delete chunks", t)
      Endpoint.fail(response, t)
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
            index.addTags(query, tags.split(","))
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

    val propertiesList = properties.split(",")

    val props = mutableMapOf<String, Any>()
    val regex = ("(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":")).toRegex()

    for (part in propertiesList.map { it.trim() }) {
      val property = part.split(regex)
      if (property.size != 2) {
        throw ServerAPIException(ServerAPIException.INVALID_PROPERTY_SYNTAX_ERROR,
            "Invalid property syntax: $part")
      }
      val key = StringEscapeUtils.unescapeJava(property[0].trim())
      val value = StringEscapeUtils.unescapeJava(property[1].trim())

      // auto-convert to numbers
      val v = value.toLongOrNull() ?: value.toDoubleOrNull() ?: value

      props[key] = v
    }

    return props
  }
}
