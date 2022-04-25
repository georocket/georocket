package io.georocket.storage

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DatabindContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.XMLStartElement
import io.vertx.core.json.JsonObject

@JsonTypeInfo(
  use = JsonTypeInfo.Id.CUSTOM,
  include = JsonTypeInfo.As.PROPERTY,
  property = "mimeType",
  visible = true,
)
@JsonTypeIdResolver(ChunkMetaResolver::class)
sealed class ChunkMeta {

  abstract val mimeType: String

  fun toJsonObject(): JsonObject {
    return JsonObject.mapFrom(this)
  }

  companion object {
    fun fromJsonObject(json: JsonObject): ChunkMeta {
      return json.mapTo(ChunkMeta::class.java)
    }
  }
}

sealed class JsonChunkMeta : ChunkMeta() {

  /**
   * The name of the field whose value is equal to this chunk or, if
   * the field is an array, whose value contains this chunk (may be
   * `null` if the chunk does not have a parent)
   */
  abstract val parentName: String?
}

sealed class XmlChunkMeta : ChunkMeta() {

  /**
   * the chunk's parents (i.e. the XML start elements the
   * chunk is wrapped in)
   */
  abstract val parents: List<XMLStartElement>
}

data class GenericChunkMeta(
  override val mimeType: String,
) : ChunkMeta()

data class GenericJsonChunkMeta constructor(
  override val parentName: String?,
  override val mimeType: String = MIME_TYPE,
) : JsonChunkMeta() {
  companion object {
    const val MIME_TYPE = "application/json"
  }

  init {
    if (!belongsTo(mimeType, "application", "json")) {
      throw IllegalArgumentException("Mime type must belong to application/json")
    }
  }
}

data class GeoJsonChunkMeta(
  /**
   * The type of the GeoJSON object represented by the chunk
   */
  val type: String,
  override val parentName: String?,
  override val mimeType: String = MIME_TYPE,
) : JsonChunkMeta() {
  companion object {
    const val MIME_TYPE = "application/geo+json"
  }

  constructor(type: String, baseMeta: JsonChunkMeta) : this(type, baseMeta.parentName, MIME_TYPE)

  init {
    if (!belongsTo(mimeType, "application", "geo+json")) {
      throw IllegalArgumentException("Mime type must belong to application/geo+json")
    }
  }
}

data class GenericXmlChunkMeta(
  override val parents: List<XMLStartElement>,
  override val mimeType: String = MIME_TYPE
) : XmlChunkMeta() {
  companion object {
    const val MIME_TYPE = "application/xml"
  }

  init {
    if (!belongsTo(mimeType, "application", "xml")) {
      throw IllegalArgumentException("Mime type must belong to application/xml")
    }
  }
}

class ChunkMetaResolver : TypeIdResolverBase() {
  var base: JavaType? = null

  override fun init(baseType: JavaType?) {
    base = baseType
  }

  override fun idFromValue(value: Any?): String = (value as ChunkMeta).mimeType

  override fun typeFromId(context: DatabindContext?, id: String?): JavaType {
    if (id == null) {
      throw NullPointerException()
    }
    if (belongsTo(id, "application", "geo+json")) {
      return context!!.constructSpecializedType(base, GeoJsonChunkMeta::class.java)
    }
    if (belongsTo(id, "application", "json")) {
      return context!!.constructSpecializedType(base, GenericJsonChunkMeta::class.java)
    }
    if (belongsTo(id, "application", "xml")) {
      return context!!.constructSpecializedType(base, GenericXmlChunkMeta::class.java)
    }
    return context!!.constructSpecializedType(base, GenericChunkMeta::class.java)
  }

  override fun idFromValueAndType(value: Any?, suggestedType: Class<*>?): String = idFromValue(value)

  override fun getMechanism(): JsonTypeInfo.Id = JsonTypeInfo.Id.CUSTOM
}
