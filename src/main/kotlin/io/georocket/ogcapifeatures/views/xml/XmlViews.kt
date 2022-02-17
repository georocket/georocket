package io.georocket.ogcapifeatures.views.xml

import com.fasterxml.jackson.core.io.IOContext
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.xml.XmlFactory
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator
import io.georocket.ogcapifeatures.views.Views
import io.vertx.core.http.HttpServerResponse
import java.io.Writer
import javax.xml.stream.XMLStreamWriter

const val XML_NAMESPACE_ATOM = "http://www.w3.org/2005/Atom"
const val XML_NAMESPACE_CORE = "http://www.opengis.net/ogcapi-features-1/1.0"

object XmlViews: Views {

  /**
   * Object mapper for xml serialisation
   */
  private val objectMapper by lazy {
    XmlMapper.builder(NamespaceXmlFactory())
      .propertyNamingStrategy(PropertyNamingStrategies.LOWER_CASE)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
      .findAndAddModules()
      .build()
  }

  /**
   * Adds the given [links] to the headers of the response.
   */
  private fun addLinkHeaders(response: HttpServerResponse, links: List<Views.Link>) {
    links.forEach { link ->
      var linkHeaderValue = "<${link.href}>"
      if (link.rel != null) {
        linkHeaderValue += "; rel=${link.rel}"
      }
      if (link.type != null) {
        linkHeaderValue += "; type=${link.type}"
      }
      response.headers().add("Link", linkHeaderValue)
    }
  }

  /**
   * Xml factory that sets custom prefix names for the xml namespaces used by the Ogc-Features API
   */
  private class NamespaceXmlFactory : XmlFactory() {
    override fun _createXmlWriter(ctxt: IOContext?, w: Writer?): XMLStreamWriter {
      val writer = super._createXmlWriter(ctxt, w)
      writer.setPrefix("atom", XML_NAMESPACE_ATOM)
      writer.setPrefix("core", XML_NAMESPACE_CORE)
      return writer
    }
  }

  override fun landingPage(response: HttpServerResponse, links: List<Views.Link>) {
    response.putHeader("Content-Type", "application/xml")
    addLinkHeaders(response, links)
    response.send(objectMapper.writeValueAsString(LandingPage(
      title = "GeoRocket OGC API Features",
      description = "Welcome to the GeoRocket OGC API Features",
      links = links
    )))
  }

  override fun conformance(response: HttpServerResponse, conformsTo: List<String>) {
    response.putHeader("content-type", "application/xml")
    response.end(objectMapper.writeValueAsString(ConformsTo(
      title = "Conformances",
      description = "List of conformance classes that this API implements.",
      links = conformsTo.map { Views.Link(it) }
    )))
  }

  override fun collections(response: HttpServerResponse, links: List<Views.Link>, collections: List<Views.Collection>) {
    response.putHeader("content-type", "application/xml")
    addLinkHeaders(response, links + collections.flatMap { it.links })
    response.end(objectMapper.writeValueAsString(Collections(
      title = "Collections",
      description = "List of all collections",
      links = links,
      collections = collections
    )))
  }

  override fun collection(response: HttpServerResponse, links: List<Views.Link>, collection: Views.Collection) {
    response.putHeader("content-type", "application/xml")
    addLinkHeaders(response, links + collection.links)
    response.end(objectMapper.writeValueAsString(
      Collections(
        title = "Collection details",
        links = links,
        collections = listOf(collection),
      )
    ))
  }
}

