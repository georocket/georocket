package io.georocket.ogcapifeatures.views

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import io.georocket.http.Endpoint
import io.georocket.ogcapifeatures.views.xml.XML_NAMESPACE_ATOM
import io.georocket.ogcapifeatures.views.xml.XML_NAMESPACE_CORE
import io.georocket.output.Merger
import io.georocket.storage.ChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServerResponse
import kotlinx.coroutines.flow.Flow
import org.slf4j.Logger

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

  object ContentTypes {
    const val JSON = "application/json"
    const val GEO_JSON = "application/geo+json"
    const val XML = "application/xml"
    const val GML_XML = "application/gml+xml"
    const val GML_SF2_XML = "application/gml+xml; version=3.2; profile=http://www.opengis.net/def/profile/ogc/2.0/gml-sf2"  // GML profile simple features level 2
  }

  fun landingPage(response: HttpServerResponse, links: List<Link>)

  fun conformance(response: HttpServerResponse, conformsTo: List<String>)

  fun collections(response: HttpServerResponse, links: List<Link>, collections: List<Collection>)

  fun collection(response: HttpServerResponse, links: List<Link>, collection: Collection)

  suspend fun items(response: HttpServerResponse, links: List<Link>, numberReturned: Int, chunks: Flow<Pair<Buffer, ChunkMeta>>)

  suspend fun item(response: HttpServerResponse, links: List<Link>, chunk: Buffer, meta: ChunkMeta, id: String)

}

suspend inline fun<reified T: ChunkMeta> mergeChunks(response: HttpServerResponse, merger: Merger<T>, chunks: Flow<Pair<Buffer, ChunkMeta>>, log: Logger) {
  try {
    var notaccepted = 0L
    chunks.collect { (chunk, chunkMeta) ->
      try {
        if (chunkMeta !is T) {
          throw IllegalStateException("Only Xml chunks allowed")
        }
        merger.merge(chunk, chunkMeta, response)
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
    merger.finish(response)
    response.end()
  } catch (t: Throwable) {
    log.error("Could not perform query", t)
    Endpoint.fail(response, t)
  }
}
