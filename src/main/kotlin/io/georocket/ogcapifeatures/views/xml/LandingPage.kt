package io.georocket.ogcapifeatures.views.xml

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement
import io.georocket.ogcapifeatures.views.Views

@JacksonXmlRootElement(namespace = XML_NAMESPACE_CORE)
data class LandingPage(
  @JacksonXmlProperty(localName = "Title", namespace = XML_NAMESPACE_CORE)
  val title: String,

  @JacksonXmlProperty(localName = "Description", namespace = XML_NAMESPACE_CORE)
  val description: String,

  @JacksonXmlElementWrapper(useWrapping = false)
  @JacksonXmlProperty(localName = "Link", namespace = XML_NAMESPACE_ATOM)
  val links: List<Views.Link>
)
