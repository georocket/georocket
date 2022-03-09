package io.georocket.storage

import io.vertx.core.json.JsonObject

/**
 * Metadata for a chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
open class ChunkMeta(
  /**
   * The chunk's mime type (typically "application/xml" or
   * "application/json")
   */
  val mimeType: String
) {

  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  constructor(json: JsonObject) : this(json.getString("mimeType", "application/xml"))

  override fun hashCode(): Int {
    val prime = 31
    var result = 1
    result = prime * result + mimeType.hashCode()
    return result
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) {
      return true
    }
    if (other == null) {
      return false
    }
    if (javaClass != other.javaClass) {
      return false
    }
    val otherChunkMeta = other as ChunkMeta
    if (mimeType != otherChunkMeta.mimeType) {
      return false
    }
    return true
  }

  /**
   * @return this object as a [JsonObject]
   */
  open fun toJsonObject(): JsonObject {
    return JsonObject().put("mimeType", mimeType)
  }
}
