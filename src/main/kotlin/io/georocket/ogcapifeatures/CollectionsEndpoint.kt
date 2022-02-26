package io.georocket.ogcapifeatures

import io.georocket.http.Endpoint
import io.georocket.index.*
import io.georocket.ogcapifeatures.views.Views
import io.georocket.ogcapifeatures.views.json.JsonViews
import io.georocket.ogcapifeatures.views.xml.XmlViews
import io.georocket.output.MultiMerger
import io.georocket.query.DefaultQueryCompiler
import io.georocket.query.IndexQuery
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.util.*
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.apache.commons.text.StringEscapeUtils.escapeJava
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.Charset
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlin.coroutines.CoroutineContext

/**
 * An endpoint to maintain collections
 * @author Michel Kraemer
 */
class CollectionsEndpoint(
  override val coroutineContext: CoroutineContext, private val vertx: Vertx
) : Endpoint, CoroutineScope {
  companion object {
    private val log = LoggerFactory.getLogger(CollectionsEndpoint::class.java)

    private const val FLOAT_REGEX_STR = """[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?"""
    private const val BBOX_REGEX_STR = "$FLOAT_REGEX_STR,$FLOAT_REGEX_STR,$FLOAT_REGEX_STR,$FLOAT_REGEX_STR"
    private val BBOX_REGEX = BBOX_REGEX_STR.toRegex()
  }

  private lateinit var config: JsonObject
  private lateinit var store: Store
  private lateinit var index: Index

  override suspend fun createRouter(): Router {
    config = vertx.orCreateContext.config()
    store = StoreFactory.createStore(vertx)
    index = IndexFactory.createIndex(vertx)

    val router = Router.router(vertx)

    // handler for list of all collections
    router.get("/").produces(Views.ContentTypes.JSON).handler { ctx -> onGetAll(ctx, JsonViews) }
    router.get("/").produces(Views.ContentTypes.XML).handler { ctx -> onGetAll(ctx, XmlViews) }
    router.get("/").handler { context ->
      respondWithHttp406NotAcceptable(context.response(), listOf(Views.ContentTypes.JSON, Views.ContentTypes.XML))
    }

    // handler for collection details
    router.get("/:collectionId").produces(Views.ContentTypes.JSON).handler { ctx -> onGet(ctx, JsonViews) }
    router.get("/:collectionId").produces(Views.ContentTypes.XML).handler { ctx -> onGet(ctx, XmlViews) }
    router.get("/:collectionId").handler { context ->
      respondWithHttp406NotAcceptable(context.response(), listOf(Views.ContentTypes.JSON, Views.ContentTypes.XML))
    }

    // handler for collection features
    router.get("/:collectionId/items").produces(Views.ContentTypes.GEO_JSON).handler { ctx -> onGetItems(ctx, JsonViews) }
    router.get("/:collectionId/items").produces(Views.ContentTypes.GML_XML).handler { ctx -> onGetItems(ctx, XmlViews) }
    router.get("/:collectionId/items").handler { context ->
      respondWithHttp406NotAcceptable(context.response(), listOf(Views.ContentTypes.GEO_JSON, Views.ContentTypes.GML_XML))
    }

    // handler for a single feature
    router.get("/:collectionId/items/:id").produces(Views.ContentTypes.GEO_JSON).handler { ctx -> onGetItemById(ctx, JsonViews) }
    router.get("/:collectionId/items/:id").produces(Views.ContentTypes.GML_XML).handler { ctx -> onGetItemById(ctx, XmlViews) }
    router.get("/:collectionId/items").handler { context ->
      respondWithHttp406NotAcceptable(context.response(), listOf(Views.ContentTypes.GEO_JSON, Views.ContentTypes.GML_XML))
    }

    return router
  }

  override suspend fun close() {
    store.close()
    index.close()
  }

  private fun compileQuery(search: String?, layer: String): IndexQuery {
    return DefaultQueryCompiler(MetaIndexerFactory.ALL + IndexerFactory.ALL)
      .compileQuery(search ?: "", layer)
  }

  /**
   * Layers with multiple path segments (separated using '/') would lead to ambiguous urls.
   * For example, consider the url "<root>/collections/foo/bar/items/" - is this the Collection end point for the
   * layer "foo/bar/items", or is it the Items end point for the layer "foo/bar"?
   * Therefor we use ':' as a path seperator for the collection id instead of '/'.
   * The conversion process is isomorphic and does not introduce additional ambiguities, because ':' is not allowed
   * as a character in layer names. As a result, we have unambiguous URLs:
   *  - <root>/collections/foo:bar/items/  for the Items end point of the layer foo/bar/
   *  - <root>/collections/foo:bar:items/  for the Collection end point of the layer foo/bar/items/
   */
  private fun layerToCollectionId(layer: String): String {
    val normalized = normalizeLayer(layer);
    return if (normalized.isEmpty()) {
      ":"
    } else {
      normalizeLayer(layer).replace('/', ':').trimEnd(':')
    }
  }

  /**
   * Inverse of [layerToCollectionId]
   */
  private fun collectionIdToLayer(collectionId: String): String =
    normalizeLayer(collectionId.replace(':', '/'))

  private suspend fun getCollectionByLayer(context: RoutingContext, layer: String): Views.Collection? {
    // List of mime types, that can be produced from the documents in the layer
    val mimeTypes = index.getDistinctMeta(compileQuery("", layer))
      .map { meta -> meta.mimeType }
      .toSet()
    val isJson = mimeTypes.all { MimeTypeUtils.belongsTo(it, "application", "json") }
    val isXml = mimeTypes.all { MimeTypeUtils.belongsTo(it, "application", "xml") }
    val mergedMimeTypes = listOf(
      Views.ContentTypes.GEO_JSON to isJson,
      Views.ContentTypes.GML_SF2_XML to isXml
    ).mapNotNull { (mimeType, isSupported) -> mimeType.takeIf { isSupported }}

    // If no mime type can be built (for example because GeoJson and Gml documents are mixed - currently, there is
    // no support for converting between the two), we exclude the whole collection from the result.
    if (mergedMimeTypes.isEmpty()) {
      return null
    }

    // build collection
    val id = layerToCollectionId(layer)
    return Views.Collection(
      id = id,
      title = if (layer.isEmpty()) { "root layer" } else { "layer at $layer" },
      links = mergedMimeTypes.map { mimeType ->
        Views.Link(
          href = PathUtils.join(context.mountPoint() ?: "/", id, "items"),
          type = mimeType,
          rel = "items"
        )
      }
    )
  }

  /**
   * Handles requests to 'GET <root>/collections/'
   */
  private fun onGetAll(ctx: RoutingContext, views: Views) {
    val response = ctx.response()
    launch {
      val layers = index.getLayers()
        .toList()
        .flatMap { layer ->
          // also include all parent layers of the layers that directly contain data
          val segments = layer.trim('/').split("/").map { "$it/" }
          (0 .. segments.size).map { index ->
            segments.subList(0, index).joinToString("")
          }
        }
        .map { normalizeLayer(it) }
        .toSet()
        .sorted()
      val collections = layers
        .map { layer ->
          async { // get the collections in parallel
            getCollectionByLayer(ctx, layer)
          }
        }
        .awaitAll()
        .filterNotNull()

      views.collections(response, Endpoint.getLinksToSelf(ctx), collections)
    }

  }

  /**
   * Handles requests to 'GET <root>/collections/{collectionId}/'
   */
  private fun onGet(ctx: RoutingContext, views: Views) {
    val response = ctx.response()

    val collectionId = ctx.pathParam("collectionId")
    if (collectionId == null) {
      response.setStatusCode(400).end("No collection name given.")
      return
    }

    launch {
      try {
        val layer = collectionIdToLayer(collectionId)
        val exists = index.existsLayer(layer)
        if (exists) {
          val collection = getCollectionByLayer(ctx, layer)
              ?: throw HttpException(404, "The collection `$collectionId' exist, but is unavailable because it mixes incompatible mime types.")
          views.collection(response, Endpoint.getLinksToSelf(ctx), collection)
        } else {
          throw HttpException(404, "The collection `$collectionId' does not exist")
        }
      } catch (t: Throwable) {
        Endpoint.fail(response, t)
      }
    }
  }

  private fun processQuery(search: String, name: String, response: HttpServerResponse) {
    launch {
      response.isChunked = true

      try {
        val query = compileQuery(search, "/$name")

        // initialize merger
        val merger = MultiMerger(false)
        val distinctMetas = index.getDistinctMeta(query)
        distinctMetas.collect { merger.init(it) }

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
            log.warn("", e)
            notaccepted++
          }
        }

        if (notaccepted > 0) {
          log.warn("Could not merge $notaccepted chunks.")
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
  }

  /**
   * Handles requests to 'GET <root>/collections/{collectionId}/items/'
   */
  private fun onGetItems(ctx: RoutingContext, views: Views) {
    val response = ctx.response()

    // get layer from path segment
    val collectionId = ctx.pathParam("collectionId")
    if (collectionId == null) {
      response.setStatusCode(400).end("No collection name given.")
      return
    }
    val layer = collectionIdToLayer(collectionId)

    // get query parameters
    val search = mutableListOf<String>()
    var limit = 100
    var prevScrollId: String? = null
    ctx.queryParams().forEach { (key, value) ->
      when(key) {
        "limit" -> {
          try {
            limit = value.toInt()
          } catch (_: java.lang.NumberFormatException) {
            response.setStatusCode(400).end("Parameter `limit' must be a number")
            return
          }
        }
        "bbox" -> {
          if (BBOX_REGEX matches value) {
            search += "\"${escapeJava(value)}\""
          } else {
            response.setStatusCode(400).end("Parameter `bbox' must contain four floating point " +
              "numbers separated by a comma")
            return
          }
        }
        "datetime" -> {
          // GeoRocket currently does not do any sort of temporal indexing.
          // Thus, the datetime parameter has no effect on the query result.
          // However, in order to conform to the ogc-api-features spec,
          // we still have to validate the parameter if it is defined.
          // see: http://docs.opengeospatial.org/is/17-069r3/17-069r3.html#_parameter_datetime
          var valid = true
          val intervalParts = value.split('/')
          if (intervalParts.size == 1) {
            try {
              DateTimeFormatter.ISO_DATE_TIME.parse(value)
            } catch (_: DateTimeParseException) {
              valid = false
            }
          } else if (intervalParts.size == 2) {
            if (intervalParts == listOf("..", "..")) {
              valid = false
            }
            for (part in intervalParts.filter { it != ".." }) {
              try {
                DateTimeFormatter.ISO_DATE_TIME.parse(part)
              } catch (_: DateTimeParseException) {
                valid = false
              }
            }
          }
          if (!valid) {
            response.setStatusCode(400).end("Parameter `datetime' must contain a valid date-time (ISO 8601) " +
              "or time interval.")
            return
          }
        }
        "scrollId" -> {
          prevScrollId = value
        }
        else -> search.add("EQ(\"${escapeJava(key)}\" \"${escapeJava(value)}\")")
      }
    }

    // assemble query
    val joinedSearch = if (search.isEmpty()) {
      ""
    } else if (search.size == 1) {
      search.first()
    } else {
      "AND(${search.joinToString(" ")})"
    }

    // query
    launch {
      val q = compileQuery(joinedSearch, layer)
      val result = index.getPaginatedMeta(q, limit, prevScrollId)

      val contentType = when (ctx.acceptableContentType) {
        Views.ContentTypes.GML_XML -> Views.ContentTypes.GML_SF2_XML
        else -> ctx.acceptableContentType
      }
      val linkToSelf = Views.Link(
        href = ctx.request().uri(),
        type = contentType,
        rel = "self"
      )
      val linkToNext = if (result.scrollId != null ) {
        val params = ctx.queryParams()
        params.set("scrollId", result.scrollId)
        val queryStr = params.map { (key, value) ->
          val encodedKey = URLEncoder.encode(key, "UTF-8")
          val encodedVal = URLEncoder.encode(value, "UTF-8")
          "$encodedKey=$encodedVal"
        }.joinToString("&")
        Views.Link(
          href = ctx.request().path() + "?" + queryStr,
          type = contentType,
          rel = "next"
        )
      } else {
        null
      }
      val chunks = result.items.asFlow().map { (path, meta) ->
        store.getOne(path) to meta
      }
      views.items(response, listOfNotNull(linkToSelf, linkToNext), result.items.size, chunks)
    }
  }

  /**
   * Get a single item from a collection by its ID
   * @param ctx the current routing context
   */
  private fun onGetItemById(ctx: RoutingContext, views: Views) {
    val response = ctx.response()

    val collectionId = ctx.pathParam("collectionId")
    if (collectionId == null) {
      response.setStatusCode(400).end("No collection name given.")
      return
    }
    val layer = collectionIdToLayer(collectionId)

    val id = ctx.pathParam("id")
    if (id == null) {
      response.setStatusCode(400).end("No feature id given.")
      return
    }

    // links
    val contentType = when (ctx.acceptableContentType) {
      Views.ContentTypes.GML_XML -> Views.ContentTypes.GML_SF2_XML
      else -> ctx.acceptableContentType
    }
    val links = listOf(
      Views.Link(
        href = ctx.request().uri(),
        type = contentType,
        rel = "self"
      ),
      Views.Link(
        href = PathUtils.join(ctx.mountPoint() ?: "/", collectionId),
        type = Views.ContentTypes.JSON,
        rel = "collection"
      ),
      Views.Link(
        href = PathUtils.join(ctx.mountPoint() ?: "/", collectionId),
        type = Views.ContentTypes.XML,
        rel = "collection"
      ),
    )

    // build query string to search for item with that id
    val encodedId = URLEncoder.encode(id, "UTF-8")
    val search = when (ctx.acceptableContentType) {
      Views.ContentTypes.GEO_JSON -> "EQ(geoJsonFeatureId $encodedId)"
      Views.ContentTypes.GML_XML -> "EQ(gmlId $encodedId)"
      else -> {
        // should be unreachable, because the route is only defined for those two content types (see createRouter())
        throw Exception("Unexpected content type")
      }
    }

    // query for the feature
    launch {
      val query = compileQuery(search, layer)
      val result = index.getPaginatedMeta(query, 1, null)
      if (result.items.isEmpty()) {
        response.setStatusCode(404).end()
        return@launch
      }
      val (path, meta) = result.items.first()
      val chunk = store.getOne(path)
      views.item(response, links, chunk, meta, id)
    }



  }
}
