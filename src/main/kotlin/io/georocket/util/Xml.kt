package io.georocket.util

import org.apache.commons.text.StringEscapeUtils

class XmlContext {
  private val writer = StringBuilder();
  private var indentDepth = 0
  private val indent get() = " ".repeat(indentDepth * 4)

  private fun attributes(attrs: Array<out Pair<String, String?>>) {
    attrs.forEach { (attr, value) ->
      if (value != null) {
        this.writer.append(" $attr=\"${StringEscapeUtils.escapeXml10(value)}\"")
      }
    }
  }

  fun xmlDeclaration(version: String = "1.0", encoding: String = "UTF-8") {
    this.writer.append("<?xml version=\"$version\" encoding=\"$encoding\"?>\n")
  }

  fun element(name: String, vararg attrs: Pair<String, String?>, children: XmlContext.() -> Unit) {
    this.writer.append("$indent<$name")
    this.attributes(attrs)
    this.writer.append(">\n")
    indentDepth += 1;
    this.apply(children)
    indentDepth -= 1;
    this.writer.append("$indent</$name>\n")
  }

  fun element(name: String, vararg attrs: Pair<String, String?>) {
    this.writer.append("$indent<$name")
    this.attributes(attrs)
    this.writer.append("/>\n")
  }

  fun text(contents: String) {
    this.writer.append(StringEscapeUtils.escapeXml10(contents).prependIndent(indent))
    this.writer.append("\n")
  }

  fun done() : String {
    return this.writer.toString()
  }

}

class XmlElement(private val markup: String) {
  override fun toString(): String {
    return markup
  }
}

/**
 * Creates an XML document,
 * that can be written in an intuitive kotlin-dsl style.
 *
 * Example:
 *
 * ```kotlin
 * xmlDocument {
 *   element("html") {
 *     element("header") {
 *       element("link", "rel" to "stylesheet", "type" to "text/css", "src" to "/styles/styles.css")
 *     }
 *     element("body") {
 *       element("h1") { text("It works!") }
 *       element("p") {
 *         text("Lorem ipsum dolor sit amet ...")
 *         element("a", "href" to "https://example.com/") {
 *           text("Click here")
 *         }
 *       }
 *     }
 *   }
 * }
 * ```
 */
fun xmlDocument(version: String = "1.0", encoding: String = "UTF-8", tags: XmlContext.() -> Unit): String {
  val context = XmlContext()
  context.xmlDeclaration(version, encoding)
  context.apply(tags)
  return context.done()
}
