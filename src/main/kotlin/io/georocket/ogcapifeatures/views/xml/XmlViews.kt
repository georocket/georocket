package io.georocket.ogcapifeatures.views.xml

import io.georocket.ogcapifeatures.views.Views
import io.georocket.util.*
import io.vertx.core.http.HttpServerResponse

object XmlViews: Views {

  /**
   * While optional for the 'GeoJSON' and 'Core' requirements classes, adding link headers is mandatory for
   * the 'Geography Markup Language (GML), Simple Features Profile, Level 0' and level 2 requirements classes
   * of the OGC-Api-Features.
   * This utility method creates a 'atom:link' tag and adds the corresponding header to the response at the same time.
   */
  private fun AtomXmlTags.link(response: HttpServerResponse, link: Views.Link) {
    var linkHeaderValue = "<${link.href}>"
    if (link.rel != null) {
      linkHeaderValue += "; rel=${link.rel}"
    }
    if (link.type != null) {
      linkHeaderValue += "; type=${link.type}"
    }
    response.headers().add("Link", linkHeaderValue)
    this.link(rel = link.rel, type = link.type, title = link.title, href = link.href)
  }

  private fun CoreXmlTags.collection(response: HttpServerResponse, collection: Views.Collection) =
    this.collection() {
      core.id() {
        text(collection.id)
      }
      collection.links.forEach { link ->
        atom.link(response, link)
      }
    }

  override fun landingPage(response: HttpServerResponse, links: List<Views.Link>) {
    val o = xmlDocument {
      core.landingPage(
        core.xmlnsAttr,
        atom.xmlnsAttr
      ) {
        core.title("xml:lang" to "en") {
          text("GeoRocket OGC API")
        }
        links.forEach { link ->
          atom.link(response, link)
        }
      }
    }
    response.putHeader("Content-Type", "application/xml")
    response.send(o)
  }

  override fun conformances(response: HttpServerResponse, conformsTo: List<String>) {
    response.putHeader("content-type", "application/xml")
    response.end(xmlDocument {
      core.conformsTo(core.xmlnsAttr, atom.xmlnsAttr) {
        conformsTo.forEach { uri ->
          atom.link(response, Views.Link(href = uri))
        }
      }
    })
  }

  override fun collections(response: HttpServerResponse, links: List<Views.Link>, collections: List<Views.Collection>) {
    response.putHeader("content-type", "application/xml")
    response.end(xmlDocument {
      core.collections(core.xmlnsAttr, atom.xmlnsAttr) {
        links.forEach { link ->
          atom.link(response, link)
        }
        collections.forEach { collection ->
          core.collection(response, collection)
        }
      }
    })
  }
}
