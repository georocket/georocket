package io.georocket.storage

import io.vertx.core.json.JsonObject

/**
 * Metadata for a GeoJSON chunk
 * @since 1.0.0
 * @author Michel Kraemer
 */
class GeoJsonChunkMeta : JsonChunkMeta {

  /**
   * The type of the GeoJSON object represented by the chunk
   */
  val type: String

  /**
   * Create a new metadata object
   * @param type the type of the GeoJSON object represented by the chunk
   * @param parentFieldName the name of the field whose value is equal to this
   * chunk or, if the field is an array, whose value contains this chunk (may
   * be `null` if the chunk does not have a parent)
   */
  constructor(type: String, parentFieldName: String?) : super(parentFieldName, MIME_TYPE) {
    this.type = type
  }

  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  constructor(json: JsonObject) : super(json) {
    type = json.getString("type")
  }

  /**
   * Makes a copy of the given [JsonChunkMeta] object but assigns a
   * GeoJSON object type
   * @param type the type of the GeoJSON object represented by the chunk
   * @param chunkMeta the chunk metadata object to copy
   */
  constructor(type: String, chunkMeta: JsonChunkMeta) : super(chunkMeta.parentFieldName, MIME_TYPE) {
    this.type = type
  }

  override fun hashCode(): Int {
    val prime = 31
    var result = super.hashCode()
    result = prime * result + type.hashCode()
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
    val otherGeoJsonMeta = other as GeoJsonChunkMeta
    if (type != otherGeoJsonMeta.type) {
      return false
    }
    return true
  }

  /**
   * @return this object as a [JsonObject]
   */
  override fun toJsonObject(): JsonObject {
    val r = super.toJsonObject()
    r.put("type", type)
    return r
  }

  companion object {
    /**
     * The mime type for GeoJSON chunks
     */
    const val MIME_TYPE = "application/geo+json"
  }
}
