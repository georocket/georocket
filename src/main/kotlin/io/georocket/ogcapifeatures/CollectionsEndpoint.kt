package io.georocket.ogcapifeatures

import io.georocket.constants.ConfigConstants
import io.georocket.http.Endpoint
import io.georocket.index.Index
import io.georocket.index.IndexerFactory
import io.georocket.index.MetaIndexerFactory
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.output.MultiMerger
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.util.HttpException
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.apache.commons.text.StringEscapeUtils.escapeJava
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.net.URLEncoder
import kotlin.coroutines.CoroutineContext

/**
 * An endpoint to maintain collections
 * @author Michel Kraemer
 */
class CollectionsEndpoint(override val coroutineContext: CoroutineContext,
    private val vertx: Vertx) : Endpoint, CoroutineScope {
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
    index = MongoDBIndex.create(vertx)

    val router = Router.router(vertx)
    router.get("/").handler(::onGetAll)
    router.get("/:name").handler(::onGet)
    val postMaxSize = config.getLong(ConfigConstants.HTTP_POST_MAX_SIZE, -1L)
    router.post("/").handler(BodyHandler.create().setBodyLimit(postMaxSize)).handler(::onPost)
    router.delete("/:name").handler(::onDelete)

    router.get("/:name/items").handler(::onGetItems)
    router.get("/:name/items/:id").handler(::onGetItemById)

    return router
  }

  override suspend fun close() {
    store.close()
    index.close()
  }

  private fun compileQuery(search: String?, path: String): JsonObject {
    return DefaultQueryCompiler(MetaIndexerFactory.ALL + IndexerFactory.ALL)
      .compileQuery(search ?: "", path)
  }

  private fun itemToJson(name: String, path: String): JsonObject {
    return jsonObjectOf(
      "name" to name,
      "links" to jsonArrayOf(
        jsonObjectOf(
          "href" to PathUtils.join(path, "items"),
          "rel" to "item",
          "type" to "application/gml+xml"
        )
      )
    )
  }

  private fun onGetAll(ctx: RoutingContext) {
    val request = ctx.request()
    val response = ctx.response()

    response.putHeader("content-type", "application/json")

    launch {
      try {
        val r = jsonObjectOf(
          "links" to jsonArrayOf(
            jsonObjectOf(
              "href" to request.path(),
              "rel" to "self",
              "type" to "application/json",
              "title" to "this document"
            )
          ),
          "collections" to index.getCollections().map {
            itemToJson(it, PathUtils.join(request.path(), it)) }.toList()
        )

        response.end(r.encode())
      } catch (t: Throwable) {
        Endpoint.fail(response, t)
      }
    }
  }

  private fun onPost(ctx: RoutingContext) {
    val response = ctx.response()
    val body = ctx.bodyAsJson
    val name = body.getString("name")

    if (name == null) {
      response
          .setStatusCode(400)
          .end("JSON object must have a name.")
      return
    }

    launch {
      try {
        if (index.existsCollection(name)) {
          throw HttpException(409, "A collection with this name already exists")
        }
        index.addCollection(name)
        response.setStatusCode(204).end()
      } catch (t: Throwable) {
        Endpoint.fail(response, t)
      }
    }
  }

  private fun onGet(ctx: RoutingContext) {
    val response = ctx.response()

    val name = ctx.pathParam("name")
    if (name == null) {
      response
        .setStatusCode(400)
        .end("No collection name given.")
      return
    }

    launch {
      try {
        val exists = index.existsCollection(name)
        if (exists) {
          response
            .putHeader("content-type", "application/json")
            .setStatusCode(200)
            .end(itemToJson(name, ctx.request().path()).encode())
        } else {
          throw HttpException(404, "The collection `$name' does not exist")
        }
      } catch (t: Throwable) {
        Endpoint.fail(response, t)
      }
    }
  }

  private fun onDelete(ctx: RoutingContext) {
    val response = ctx.response()

    val name = ctx.pathParam("name")
    if (name == null) {
      response
        .setStatusCode(400)
        .end("No collection name given.")
      return
    }

    launch {
      try {
        // delete chunks in the given layer in index and store
        val query = compileQuery(null, "/$name")
        val paths = index.getPaths(query)
        store.delete(paths)
        index.delete(query)

        // delete collection
        index.deleteCollection(name)

        response
          .setStatusCode(204) // No Content
          .end()
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
   * Get items from a collection
   */
  private fun onGetItems(ctx: RoutingContext) {
    val response = ctx.response()

    val name = ctx.pathParam("name")
    if (name == null) {
      response
        .setStatusCode(400)
        .end("No collection name given.")
      return
    }

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
            response
                .setStatusCode(400)
                .end("Parameter `bbox' must contain four floating point " +
                    "numbers separated by a comma")
            return
          }
        }

        "limit" -> try {
          limit = value.toInt()
        } catch (e: NumberFormatException) {
          response
              .setStatusCode(400)
              .end("Parameter `limit' must be a number")
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

    processQuery(joinedSearch, name, response)
  }

  /**
   * Get a single item from a collection by its ID
   * @param ctx the current routing context
   */
  private fun onGetItemById(ctx: RoutingContext) {
    val response = ctx.response()

    val name = ctx.pathParam("name")
    if (name == null) {
      response
        .setStatusCode(400)
        .end("No collection name given.")
      return
    }

    val id = ctx.pathParam("id")
    if (id == null) {
      response
          .setStatusCode(400)
          .end("No feature id given.")
      return
    }

    val encodedId = URLEncoder.encode(id, "UTF-8")
    processQuery("EQ(gmlId $encodedId)", name, response)
  }
}
