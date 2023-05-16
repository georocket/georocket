package io.georocket.storage

import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.georocket.util.XMLStartElement
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.junit5.VertxExtension
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * @author Tobias Dorra
 */
@ExtendWith(VertxExtension::class)
class ChunkMetaTest {

  companion object {
    @BeforeAll
    @JvmStatic
    private fun setupKotlinModule() {
      DatabindCodec.mapper().registerKotlinModule()
      DatabindCodec.prettyMapper().registerKotlinModule()
    }
  }

  @Test
  fun simple() {
    assertJsonRepr(
      GenericChunkMeta("application/whatever"),
      jsonObjectOf(
        "mimeType" to "application/whatever",
      )
    )
  }

  @Test
  fun json() {
    assertJsonRepr(
      GenericJsonChunkMeta(
        parentName = "parentField"
      ),
      jsonObjectOf(
        "mimeType" to "application/json",
        "parentName" to "parentField"
      )
    )
  }

  @Test
  fun jsonWithoutParent() {
    assertJsonRepr(
      GenericJsonChunkMeta(
        parentName = null
      ),
      jsonObjectOf(
        "mimeType" to "application/json",
        "parentName" to null
      )
    )
  }

  @Test
  fun jsonWithMimeType() {
    assertJsonRepr(
      GenericJsonChunkMeta(
        parentName = "parentField",
        mimeType = "application/some+json;param=value"
      ),
      jsonObjectOf(
        "mimeType" to "application/some+json;param=value",
        "parentName" to "parentField"
      )
    )
  }

  @Test
  fun geoJson() {
    assertJsonRepr(
      GeoJsonChunkMeta(
        type = "Point",
        parentName = "features",
      ),
      jsonObjectOf(
        "mimeType" to "application/geo+json",
        "type" to "Point",
        "parentName" to "features"
      )
    )
  }

  @Test
  fun geoJsonWithMimeType() {
    assertJsonRepr(
      GeoJsonChunkMeta(
        type = "Point",
        parentName = "features",
        mimeType = "application/geo+json;parameter=value"
      ),
      jsonObjectOf(
        "mimeType" to "application/geo+json;parameter=value",
        "type" to "Point",
        "parentName" to "features"
      )
    )
  }

  @Test
  fun xml() {
    assertJsonRepr(
      GenericXmlChunkMeta(
        parents = listOf(
          XMLStartElement(localName = "parentTag"),
        )
      ),
      jsonObjectOf(
        "mimeType" to "application/xml",
        "parents" to jsonArrayOf(
          jsonObjectOf(
            "prefix" to null,
            "localName" to "parentTag",
            "namespacePrefixes" to jsonArrayOf(),
            "namespaceUris" to jsonArrayOf(),
            "attributePrefixes" to jsonArrayOf(),
            "attributeLocalNames" to jsonArrayOf(),
            "attributeValues" to jsonArrayOf(),
          )
        )
      )
    )
  }

  @Test
  fun xmlGml() {
    assertJsonRepr(
      GenericXmlChunkMeta(
        mimeType = "application/gml+xml",
        parents = listOf(
          XMLStartElement(
            prefix = "gml",
            localName = "Point",
            namespacePrefixes = listOf("gml"),
            namespaceUris = listOf("http://www.opengis.net/gml"),
            attributePrefixes = listOf(null),
            attributeLocalNames = listOf("srsDimension"),
            attributeValues = listOf("2")
          )
        )
      ),
      jsonObjectOf(
        "mimeType" to "application/gml+xml",
        "parents" to jsonArrayOf(
          jsonObjectOf(
            "prefix" to "gml",
            "localName" to "Point",
            "namespacePrefixes" to jsonArrayOf("gml"),
            "namespaceUris" to jsonArrayOf("http://www.opengis.net/gml"),
            "attributePrefixes" to jsonArrayOf(null),
            "attributeLocalNames" to jsonArrayOf("srsDimension"),
            "attributeValues" to jsonArrayOf("2"),
          )
        )
      )
    )
  }

  private fun assertJsonRepr(value: ChunkMeta, json: JsonObject) {

    // test value to json
    val encoded = value.toJsonObject()
    assertThat(encoded).isEqualTo(json)

    // test json to value
    val decoded = ChunkMeta.fromJsonObject(json)
    assertThat(decoded).isEqualTo(value)
  }
}
