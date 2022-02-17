package io.georocket.ogcapifeatures.views.json

import io.georocket.ogcapifeatures.views.Views
import io.georocket.util.PathUtils
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.*

object JsonViews: Views {

  private fun linkToJson(link: Views.Link): JsonObject {
    val json = JsonObject()
    json.put("href", link.href)
    if (link.rel != null) {
      json.put("rel", link.rel)
    }
    if (link.type != null) {
      json.put("type", link.type)
    }
    if (link.title != null) {
      json.put("title", link.title)
    }
    return json
  }

  private fun collectionToJson(collection: Views.Collection): JsonObject {
    val json = jsonObjectOf(
      "id" to collection.id,
      "links" to collection.links.map(this::linkToJson)
    )
    json.put("title", collection.title)
    return json
  }

  override fun landingPage(response: HttpServerResponse, links: List<Views.Link>) {
    val o = jsonObjectOf(
      "title" to "GeoRocket OGC API Features",
      "description" to "Welcome to the GeoRocket OGC API Features",
      "links" to links.map(this::linkToJson)
    )
    response.putHeader("Content-Type", "application/json")
    response.end(o.encodePrettily())
  }

  override fun conformance(response: HttpServerResponse, conformsTo: List<String>) {
    val o = jsonObjectOf(
      "title" to "Conformances",
      "description" to "List of conformance classes that this API implements.",
      "conformsTo" to conformsTo
    ).encodePrettily()
    response.putHeader("content-type", "application/json")
    response.end(o)
  }

  override fun collections(response: HttpServerResponse, links: List<Views.Link>, collections: List<Views.Collection>) {
    val o = jsonObjectOf(
      "title" to "Collections",
      "description" to "List of all collections",
      "links" to links.map(this::linkToJson),
      "collections" to collections.map(this::collectionToJson)
    ).encodePrettily()
    response.putHeader("content-type", "application/json")
    response.end(o)
  }

  override fun collection(response: HttpServerResponse, links: List<Views.Link>, collection: Views.Collection) {
    val o = collectionToJson(
      collection.copy(links = links + collection.links)
    ).encodePrettily()

    response.putHeader("content-type", "application/json")
    response.end(o)
  }
}
