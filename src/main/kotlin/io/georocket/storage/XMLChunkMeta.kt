package io.georocket.storage

import io.georocket.util.XMLStartElement
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.stream.Collectors

/**
 * Metadata for an XML chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
class XMLChunkMeta : ChunkMeta {
  /**
   * @return the chunk's parents (i.e. the XML start elements the
   * chunk is wrapped in)
   */
  val parents: List<XMLStartElement>

  /**
   * Create a new metadata object
   * @param parents the chunk's parents (i.e. the XML start elements the
   * chunk is wrapped in)
   */
  constructor(parents: List<XMLStartElement>) : super(MIME_TYPE) {
    this.parents = parents
  }

  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  constructor(json: JsonObject) : super(json) {
    parents = json.getJsonArray("parents").stream()
      .map { e -> XMLStartElement.fromJsonObject(JsonObject.mapFrom(e)) }
      .collect(Collectors.toList())
  }

  override fun hashCode(): Int {
    val prime = 31
    var result = super.hashCode()
    result = prime * result + parents.hashCode()
    return result
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) {
      return true
    }
    if (!super.equals(other)) {
      return false
    }
    if (javaClass != other.javaClass) {
      return false
    }
    val otherXmlChunkMeta = other as XMLChunkMeta
    if (parents != otherXmlChunkMeta.parents) {
      return false
    }
    return true
  }

  /**
   * @return this object as a [JsonObject]
   */
  override fun toJsonObject(): JsonObject {
    val ps = JsonArray()
    parents.forEach { ps.add(it.toJsonObject()) }
    return super.toJsonObject()
      .put("parents", ps)
  }

  companion object {
    /**
     * The mime type for XML chunks
     */
    const val MIME_TYPE = "application/xml"
  }
}
