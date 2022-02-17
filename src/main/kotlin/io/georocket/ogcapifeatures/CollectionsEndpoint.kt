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
import java.net.URLEncoder
import kotlin.coroutines.CoroutineContext
import kotlin.streams.toList

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
    router.get("/").produces("application/json").handler { ctx -> onGetAll(ctx, JsonViews) }
    router.get("/").produces("application/xml").handler { ctx -> onGetAll(ctx, XmlViews) }
    router.get("/").handler { context ->
      respondWithHttp406NotAcceptable(context, listOf("application/json", "application/xml"))
    }

    // handler for collection details
    router.get("/:collectionId").produces("application/json").handler { ctx -> onGet(ctx, JsonViews) }
    router.get("/:collectionId").produces("application/xml").handler { ctx -> onGet(ctx, XmlViews) }
    router.get("/:collectionId").handler { context ->
      respondWithHttp406NotAcceptable(context, listOf("application/json", "application/xml"))
    }

    router.get("/:collectionId/items").handler(::onGetItems)
    router.get("/:collectionId/items/:id").handler(::onGetItemById)

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



  private suspend fun getCollectionByLayer(context: RoutingContext, layer: String): Views.Collection {
    // Guess the mime type, that the merger will produce for this layer
    val mimeTypes = index.getDistinctMeta(compileQuery("", layer))
      .map { meta -> meta.mimeType }
      .toSet()
    val isJson = mimeTypes.all { MimeTypeUtils.belongsTo(it, "application", "json") }
    val isXml = mimeTypes.all { MimeTypeUtils.belongsTo(it, "application", "xml") }
    val mimeType = if (isJson) {
      "application/geo+json"
    } else if (isXml) {
      "application/gml+xml; version=3.2; profile=http://www.opengis.net/def/profile/ogc/2.0/gml-sf2"
    } else if (mimeTypes.size == 1) {
      mimeTypes.first()
    }  else {
      "application/octet-stream"
    }

    // build collection
    val id = layerToCollectionId(layer)
    return Views.Collection(
      id = id,
      title = if (layer.isEmpty()) { "root layer" } else { "layer at $layer" },
      links = listOf(
        Views.Link(
          href = PathUtils.join(context.mountPoint() ?: "/", id, "items"),
          type = mimeType,
          rel = "items"
        )
      )
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
      val collections = layers.map { layer ->
        async { // get the collections in parallel
          getCollectionByLayer(ctx, layer)
        }
      }.awaitAll()

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
  private fun onGetItems(ctx: RoutingContext) {
    val response = ctx.response()

    val collectionId = ctx.pathParam("collectionId")
    if (collectionId == null) {
      response.setStatusCode(400).end("No collection name given.")
      return
    }
    val layer = collectionIdToLayer(collectionId)

    // TODO respect 'limit' parameter
    var limit = 10
    val search = mutableListOf<String>()

    // translate params to GeoRocket query
    ctx.queryParams().forEach { e ->
      val key = e.key
      val value = e.value
      when (key) {
        "bbox" -> {
          if (BBOX_REGEX matches value) {
            search += "\"${escapeJava(value)}\""
          } else {
            response.setStatusCode(400).end(
                "Parameter `bbox' must contain four floating point " + "numbers separated by a comma"
              )
            return
          }
        }

        "limit" -> try {
          limit = value.toInt()
        } catch (e: NumberFormatException) {
          response.setStatusCode(400).end("Parameter `limit' must be a number")
          return
        }

        else -> search.add("EQ(\"${escapeJava(key)}\" \"${escapeJava(value)}\")")
      }
    }

    val joinedSearch = if (search.isEmpty()) {
      ""
    } else if (search.size == 1) {
      search.first()
    } else {
      "AND(${search.joinToString(" ")})"
    }

    processQuery(joinedSearch, layer, response)
  }

  /**
   * Get a single item from a collection by its ID
   * @param ctx the current routing context
   */
  private fun onGetItemById(ctx: RoutingContext) {
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

    val encodedId = URLEncoder.encode(id, "UTF-8")
    processQuery("EQ(gmlId $encodedId)", layer, response)
  }
}
