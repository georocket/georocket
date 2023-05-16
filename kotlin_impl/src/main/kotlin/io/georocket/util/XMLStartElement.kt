package io.georocket.util

import com.fasterxml.jackson.annotation.JsonIgnore
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * A simple class describing an XML start element with optional prefix,
 * namespaces and attributes.
 * @since 1.0.0
 * @author Michel Kraemer
 */
data class XMLStartElement constructor(
  val prefix: String? = null,
  val localName: String,
  val namespacePrefixes: List<String?> = emptyList(),
  val namespaceUris: List<String> = emptyList(),
  val attributePrefixes: List<String?> = emptyList(),
  val attributeLocalNames: List<String> = emptyList(),
  val attributeValues: List<String> = emptyList()
) {

  init {
    require(
      namespacePrefixes.size == namespaceUris.size
    ) {
      "namespacePrefixes and namespaceUris must have the same number of elements"
    }
    require(
      attributePrefixes.size == attributeLocalNames.size && attributePrefixes.size == attributeValues.size
    ) {
      "attributePrefixes, attributeLocalNames and attributeValues must have the same number of elements"
    }
    require(prefix == null  || prefix.isNotEmpty()) {
      "prefix must not be empty - to indicate that there is no prefix use the `null` value"
    }
    require(namespacePrefixes.all { it == null || it.isNotEmpty() }) {
      "namespace prefixes must not be empty - use the `null` value to refer to the default namespace"
    }
    require(attributePrefixes.all { it == null || it.isNotEmpty() }) {
      "attribute prefixes must not be empty - use the `null` value for attributes without prefix"
    }
  }

  /**
   * @return the elements name consisting of the prefix and the local name
   */
  @get:JsonIgnore
  val name: String
    get() = if (prefix != null) "$prefix:$localName" else localName

  /**
   * @return the number of namespaces attached to this element
   */
  @get:JsonIgnore
  val namespaceCount: Int
    get() = namespacePrefixes.size

  /**
   * Get a namespace prefix at a given position
   * @param i the position
   * @return the prefix
   */
  fun getNamespacePrefix(i: Int): String? {
    return namespacePrefixes[i]
  }

  /**
   * Get a namespace URI at a given position
   * @param i the position
   * @return the URI
   */
  fun getNamespaceUri(i: Int): String {
    return namespaceUris[i]
  }

  /**
   * @return the number of attributes attached to this element
   */
  @get:JsonIgnore
  val attributeCount: Int
    get() = attributePrefixes.size

  /**
   * Get an attribute prefix at a given position
   * @param i the position
   * @return the prefix
   */
  fun getAttributePrefix(i: Int): String? {
    return attributePrefixes[i]
  }

  /**
   * Get the local name of an attribute at a given position
   * @param i the position
   * @return the local name
   */
  fun getAttributeLocalName(i: Int): String {
    return attributeLocalNames[i]
  }

  /**
   * Get an attribute value at a given position
   * @param i the position
   * @return the value
   */
  fun getAttributeValue(i: Int): String {
    return attributeValues[i]
  }

  override fun toString(): String {
    val sb = StringBuilder()
    sb.append("<$name")
    for (i in 0 until namespaceCount) {
      sb.append(" xmlns")
      val prefix = getNamespacePrefix(i)
      if (prefix != null) {
        sb.append(":")
        sb.append(prefix)
      }
      sb.append("=\"")
      sb.append(getNamespaceUri(i))
      sb.append("\"")
    }
    for (i in 0 until attributeCount) {
      sb.append(" ")
      val prefix = getAttributePrefix(i)
      if (prefix != null) {
        sb.append(prefix)
        sb.append(":")
      }
      sb.append(getAttributeLocalName(i))
      sb.append("=\"")
      sb.append(getAttributeValue(i))
      sb.append("\"")
    }
    sb.append(">")
    return sb.toString()
  }

  /**
   * @return this object as a [JsonObject]
   */
  fun toJsonObject(): JsonObject {
    return jsonObjectOf(
      "prefix" to prefix,
      "localName" to localName,
      "namespacePrefixes" to namespacePrefixes,
      "namespaceUris" to namespaceUris,
      "attributePrefixes" to attributePrefixes,
      "attributeLocalNames" to attributeLocalNames,
      "attributeValues" to attributeValues,
    )
  }

  companion object {
    /**
     * Converts a [JsonObject] to a [XMLStartElement]
     * @param obj the [JsonObject] to convert
     * @return the [XMLStartElement]
     */
    fun fromJsonObject(obj: JsonObject): XMLStartElement {
      return XMLStartElement(
        prefix = obj.getString("prefix")?.takeIf { it.isNotEmpty() },
        localName = obj.getString("localName")!!,
        attributePrefixes = obj.getJsonArray("attributePrefixes").map { jsonPrefix -> jsonPrefix?.toString()?.takeIf { it.isNotEmpty() } },
        attributeLocalNames = obj.getJsonArray("attributeLocalNames").map(Any::toString),
        attributeValues = obj.getJsonArray("attributeValues").map(Any::toString),
        namespacePrefixes = obj.getJsonArray("namespacePrefixes").map { jsonPrefix -> jsonPrefix?.toString()?.takeIf { it.isNotEmpty() } },
        namespaceUris = obj.getJsonArray("namespaceUris").map(Any::toString)
      )

    }
  }
}
