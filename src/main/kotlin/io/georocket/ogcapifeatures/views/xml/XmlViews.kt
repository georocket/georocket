package io.georocket.ogcapifeatures.views.xml

import com.fasterxml.jackson.core.io.IOContext
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.xml.XmlFactory
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator
import io.georocket.ogcapifeatures.views.Views
import io.georocket.ogcapifeatures.views.mergeChunks
import io.georocket.output.xml.XMLMerger
import io.georocket.storage.ChunkMeta
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.io.BufferWriteStream
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServerResponse
import kotlinx.coroutines.flow.Flow
import org.slf4j.LoggerFactory
import org.w3c.dom.Element
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.Writer
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.stream.XMLStreamWriter
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

const val XML_NAMESPACE_ATOM = "http://www.w3.org/2005/Atom"
const val XML_NAMESPACE_CORE = "http://www.opengis.net/ogcapi-features-1/1.0"

object XmlViews: Views {

  private val log = LoggerFactory.getLogger(XmlViews::class.java)

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
    response.putHeader("Content-Type", Views.ContentTypes.XML)
    addLinkHeaders(response, links)
    response.send(objectMapper.writeValueAsString(LandingPage(
      title = "GeoRocket OGC API Features",
      description = "Welcome to the GeoRocket OGC API Features",
      links = links
    )))
  }

  override fun conformance(response: HttpServerResponse, conformsTo: List<String>) {
    response.putHeader("content-type", Views.ContentTypes.XML)
    response.end(objectMapper.writeValueAsString(ConformsTo(
      title = "Conformances",
      description = "List of conformance classes that this API implements.",
      links = conformsTo.map { Views.Link(it) }
    )))
  }

  override fun collections(response: HttpServerResponse, links: List<Views.Link>, collections: List<Views.Collection>) {
    response.putHeader("content-type", Views.ContentTypes.XML)
    addLinkHeaders(response, links + collections.flatMap { it.links })
    response.end(objectMapper.writeValueAsString(Collections(
      title = "Collections",
      description = "List of all collections",
      links = links,
      collections = collections
    )))
  }

  override fun collection(response: HttpServerResponse, links: List<Views.Link>, collection: Views.Collection) {
    response.putHeader("content-type", Views.ContentTypes.XML)
    addLinkHeaders(response, links + collection.links)
    response.end(objectMapper.writeValueAsString(
      Collections(
        title = "Collection details",
        links = links,
        collections = listOf(collection),
      )
    ))
  }

  override suspend fun items(
    response: HttpServerResponse,
    links: List<Views.Link>,
    numberReturned: Int,
    chunks: Flow<Pair<Buffer, ChunkMeta>>
  ) {

    // headers
    addLinkHeaders(response, links)
    response.putHeader("OGC-NumberReturned", numberReturned.toString())
    response.putHeader("content-type", Views.ContentTypes.GML_SF2_XML)
    response.isChunked = true

    // body is the merged xml
    val merger = XMLMerger(true)
    mergeChunks(response, merger, chunks, log)
  }

  override suspend fun item(response: HttpServerResponse, links: List<Views.Link>, chunk: Buffer, meta: ChunkMeta, id: String) {

    // use the merger to assemble the full xml document (including all parent elements)
    if (meta !is XMLChunkMeta) {
      return
    }
    val merger = XMLMerger(false)
    val mergedStream = BufferWriteStream()
    merger.init(meta)
    merger.merge(chunk, meta, mergedStream)
    merger.finish(mergedStream)
    val merged = mergedStream.buffer.bytes

    // search for the element with this gml:id in the xml document
    @Suppress("BlockingMethodInNonBlockingContext") // there is no blocking, because there is no i/o.
    val document = DocumentBuilderFactory.newInstance()
      .apply { isNamespaceAware = true }
      .newDocumentBuilder()
      .parse(ByteArrayInputStream(merged))
    fun searchInElement(el: Element): Element? {
      if (el.hasAttribute("gml:id") && el.getAttribute("gml:id") == id) {
        return el
      }
      val children = el.childNodes
      for (i in 0 until children.length) {
        val child = children.item(i)
        if (child is Element) {
          val recursiveResult = searchInElement(child)
          if (recursiveResult != null) {
            return recursiveResult
          }
        }
      }
      return null
    }
    val found = searchInElement(document.documentElement) ?: document.documentElement

    // encode the found xml element
    val resultStream = ByteArrayOutputStream()
    TransformerFactory.newInstance()
      .newTransformer()
      .transform(DOMSource(found), StreamResult(resultStream))
    val result = resultStream.toByteArray()

    // headers
    addLinkHeaders(response, links)
    response.putHeader("content-type", Views.ContentTypes.GML_SF2_XML)

    // body
    response.end(Buffer.buffer(result))
  }
}

