package io.georocket.ogcapifeatures.views

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import io.georocket.ogcapifeatures.views.xml.XML_NAMESPACE_ATOM
import io.georocket.ogcapifeatures.views.xml.XML_NAMESPACE_CORE
import io.vertx.core.http.HttpServerResponse

interface Views {

  data class Link(
    @JacksonXmlProperty(isAttribute = true)
    val href: String,

    @JacksonXmlProperty(isAttribute = true)
    val rel: String? = null,

    @JacksonXmlProperty(isAttribute = true)
    val type: String? = null,

    @JacksonXmlProperty(isAttribute = true)
    val title: String? = null
  )

  data class Collection(
    @JacksonXmlProperty(localName = "Id", namespace = XML_NAMESPACE_CORE)
    val id: String,

    @JacksonXmlProperty(localName = "Title", namespace = XML_NAMESPACE_CORE)
    val title: String,

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Link", namespace = XML_NAMESPACE_ATOM)
    val links: List<Link>,
  )

  fun landingPage(response: HttpServerResponse, links: List<Link>)

  fun conformance(response: HttpServerResponse, conformsTo: List<String>)

  fun collections(response: HttpServerResponse, links: List<Link>, collections: List<Collection>)

  fun collection(response: HttpServerResponse, links: List<Link>, collection: Collection)
}
