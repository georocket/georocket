package io.georocket.ogcapifeatures.views

import io.vertx.core.http.HttpServerResponse

interface Views {

  data class Link(
    val href: String,
    val rel: String? = null,
    val type: String? = null,
    val title: String? = null
  )

  data class Collection(
    val id: String,
    val links: List<Link>,
  )

  fun landingPage(response: HttpServerResponse, links: List<Link>)

  fun conformances(response: HttpServerResponse, conformsTo: List<String>)

  fun collections(response: HttpServerResponse, links: List<Link>, collections: List<Collection>)

  fun collection(response: HttpServerResponse, links: List<Link>, collection: Collection)
}
