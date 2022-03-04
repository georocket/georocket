package io.georocket.storage

import io.vertx.core.json.JsonObject

/**
 * Metadata for a JSON chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
open class JsonChunkMeta : ChunkMeta {

  /**
   * The name of the field whose value is equal to this chunk or, if
   * the field is an array, whose value contains this chunk (may be
   * `null` if the chunk does not have a parent)
   */
  val parentFieldName: String?

  /**
   * Create a new metadata object
   * @param parentFieldName the name of the field whose value is equal to this
   * chunk or, if the field is an array, whose value contains this chunk (may
   * be `null` if the chunk does not have a parent)
   */
  constructor(parentFieldName: String?) : super(MIME_TYPE) {
    this.parentFieldName = parentFieldName
  }

  /**
   * Create a new metadata object
   * @param parentFieldName the name of the field whose value is equal to this
   * chunk or, if the field is an array, whose value contains this chunk (may
   * be `null` if the chunk does not have a parent)
   * @param mimeType the chunk's mime type (should be "application/json" or a
   * subtype)
   */
  constructor(parentFieldName: String?, mimeType: String) : super(mimeType) {
    this.parentFieldName = parentFieldName
  }

  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  constructor(json: JsonObject) : super(json) {
    parentFieldName = json.getString("parentName")
  }

  override fun hashCode(): Int {
    val prime = 31
    var result = super.hashCode()
    result = prime * result + (parentFieldName?.hashCode() ?: 0)
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
    val otherJsonChunkMeta = other as JsonChunkMeta
    if (parentFieldName != otherJsonChunkMeta.parentFieldName) {
      return false
    }
    return true
  }

  /**
   * @return this object as a [JsonObject]
   */
  override fun toJsonObject(): JsonObject {
    val r = super.toJsonObject()
    if (parentFieldName != null) {
      r.put("parentName", parentFieldName)
    }
    return r
  }

  companion object {
    /**
     * The mime type for JSON chunks
     */
    const val MIME_TYPE = "application/json"
  }
}
