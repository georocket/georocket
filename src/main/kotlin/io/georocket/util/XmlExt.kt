package io.georocket.util

import io.vertx.core.http.HttpServerResponse
import io.vertx.ext.web.RoutingContext

/**
 * Gives access to the XML tags defined under the 'core' prefix.
 *
 * It is assumed, that the prefix 'core' is mapped to the namespace 'http://www.opengis.net/ogcapi-features-1/1.0'.
 * The helper `core.xmlnsAttr` can be used as a shorthand for the correct xmlns attribute,
 * that should be added to one of the parent tags.
 *
 * See also: http://docs.opengeospatial.org/is/17-069r3/17-069r3.html#_requirements_class_geography_markup_language_gml_simple_features_profile_level_0
 * See also: http://schemas.opengis.net/ogcapi/features/part1/1.0/xml/core.xsd
 */
val XmlContext.core get() = CoreXmlTags(this)

/**
 * Gives access to the XML tags defined under the 'atom' prefix.
 *
 * Namespace: http://www.w3.org/2005/Atom
 * Use 'atom.xmlnsAttr' for a shortcut to the correct xmlns attribute.
 *
 * See also: http://schemas.opengis.net/kml/2.3/atom-author-link.xsd
 */
val XmlContext.atom get() = AtomXmlTags(this)

/**
 * Defines tags with the 'core' prefix.
 */
class CoreXmlTags(val context: XmlContext) {
  companion object {
    private val serviceAttr = "service" to "OGCAPI-Features"
    private val versionAttr = "version" to "1.0.0"  // this is the implemented version of the 'OGC API - Features' spec.
  }

  val xmlnsAttr = "xmlns:core" to "http://www.opengis.net/ogcapi-features-1/1.0"

  fun landingPage(vararg attrs: Pair<String, String>, children: XmlContext.() -> Unit) =
    context.element("core:LandingPage", serviceAttr, versionAttr, *attrs, children = children)

  fun title(vararg attrs: Pair<String, String>, children: XmlContext.() -> Unit) =
    context.element("core:Title", attrs = attrs, children = children)

  fun conformsTo(vararg attrs: Pair<String, String>, children: XmlContext.() -> Unit) =
    context.element("core:ConformsTo", serviceAttr, versionAttr, *attrs, children = children)

  fun collections(vararg attrs: Pair<String, String>, children: XmlContext.() -> Unit) =
    context.element("core:Collections", serviceAttr, versionAttr, *attrs, children = children)

  fun collection(vararg attrs: Pair<String, String>, children: XmlContext.() -> Unit) =
    context.element("core:Collection", attrs = attrs, children = children)

  fun id(vararg attrs: Pair<String, String>, children: XmlContext.() -> Unit) =
    context.element("core:Id", attrs = attrs, children = children)
}

/**
 * Defines tags with the 'atom' prefix.
 */
class AtomXmlTags(val context: XmlContext) {

  val xmlnsAttr = "xmlns:atom" to "http://www.w3.org/2005/Atom"

  fun link(href: String, rel: String? = null, type: String? = null, title: String? = null, vararg attrs: Pair<String, String>) =
    context.element("atom:link",
      "href" to href,
      "rel" to rel,
      "type" to type,
      "title" to title,
      *attrs)

}
