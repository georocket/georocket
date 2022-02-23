package io.georocket.output.geojson

import io.georocket.output.Merger
import io.georocket.storage.GeoJsonChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.core.streams.WriteStream
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * Merges chunks to valid GeoJSON documents
 * @param optimistic `true` if chunks should be merged optimistically
 * without prior initialization. In this mode, the merger will always return
 * `FeatureCollection`s.
 * @author Michel Kraemer
 */
class GeoJsonMerger(optimistic: Boolean, private val extensionProperties: JsonObject = jsonObjectOf()) : Merger<GeoJsonChunkMeta> {
  companion object {
    private const val NOT_SPECIFIED = 0
    private const val GEOMETRY_COLLECTION = 1
    private const val FEATURE_COLLECTION = 2
    private val TRANSITIONS = listOf(
        listOf(FEATURE_COLLECTION, GEOMETRY_COLLECTION),
        listOf(FEATURE_COLLECTION, GEOMETRY_COLLECTION),
        listOf(FEATURE_COLLECTION, FEATURE_COLLECTION)
    )
    private val RESERVED_PROPERTY_NAMES = listOf("type", "features", "geometries")
  }

  /**
   * `true` if [merge] has been called at least once
   */
  private var mergeStarted = false

  /**
   * True if the header has already been written in [merge]
   */
  private var headerWritten = false

  /**
   * The GeoJSON object type the merged result should have
   */
  private var mergedType = if (optimistic) FEATURE_COLLECTION else NOT_SPECIFIED

  init {
    // check, that all passed extension properties are valid.
    val usesReservedProperty = RESERVED_PROPERTY_NAMES.any {
      extensionProperties.containsKey(it)
    }
    if (usesReservedProperty) {
      throw IllegalArgumentException("One of the extension properties is invalid, because the property " +
        "names \"${RESERVED_PROPERTY_NAMES.joinToString("\", \"")}\" are reserved.")
    }
  }

  private fun writeExtensionProperties(outputStream: WriteStream<Buffer>) {
    for ((key, value) in extensionProperties) {
      outputStream.write(Json.encodeToBuffer(key))
      outputStream.write(Buffer.buffer(":"))
      outputStream.write(Json.encodeToBuffer(value))
      outputStream.write(Buffer.buffer(","))
    }
  }

  /**
   * Write the header to the given [outputStream]
   */
  private fun writeHeader(outputStream: WriteStream<Buffer>) {
    outputStream.write(Buffer.buffer("{"))
    writeExtensionProperties(outputStream)
    if (mergedType == FEATURE_COLLECTION) {
      outputStream.write(Buffer.buffer("\"type\":\"FeatureCollection\",\"features\":["))
    } else if (mergedType == GEOMETRY_COLLECTION) {
      outputStream.write(Buffer.buffer("\"type\":\"GeometryCollection\",\"geometries\":["))
    }
  }

  override fun init(chunkMetadata: GeoJsonChunkMeta) {
    if (mergeStarted) {
      throw IllegalStateException("You cannot initialize the merger anymore " +
          "after merging has begun")
    }
    if (mergedType == FEATURE_COLLECTION) {
      // shortcut: we don't need to analyse the other chunks anymore,
      // we already reached the most generic type
      return
    }

    // calculate the type of the merged document
    mergedType = if ("Feature" == chunkMetadata.type) {
      TRANSITIONS[mergedType][0]
    } else {
      TRANSITIONS[mergedType][1]
    }
  }

  override suspend fun merge(chunk: Buffer, chunkMetadata: GeoJsonChunkMeta,
      outputStream: WriteStream<Buffer>) {
    mergeStarted = true
    if (!headerWritten) {
      writeHeader(outputStream)
      headerWritten = true
    } else {
      if (mergedType == FEATURE_COLLECTION || mergedType == GEOMETRY_COLLECTION) {
        outputStream.write(Buffer.buffer(","))
      } else {
        throw IllegalStateException("Trying to merge two or more chunks but " +
            "the merger has only been initialized with one chunk.")
      }
    }

    // check if we have to wrap a geometry into a feature
    val wrap = mergedType == FEATURE_COLLECTION && "Feature" != chunkMetadata.type
    if (wrap) {
      outputStream.write(Buffer.buffer("{\"type\":\"Feature\",\"geometry\":"))
    }

    outputStream.write(chunk)

    if (wrap) {
      outputStream.write(Buffer.buffer("}"))
    }
  }

  override fun finish(outputStream: WriteStream<Buffer>) {
    if (!headerWritten) {
      writeHeader(outputStream)
    }
    if (mergedType == FEATURE_COLLECTION || mergedType == GEOMETRY_COLLECTION) {
      outputStream.write(Buffer.buffer("]}"))
    }
  }
}
