package io.georocket.output

import io.georocket.output.geojson.GeoJsonMerger
import io.georocket.output.xml.XMLMerger
import io.georocket.storage.ChunkMeta
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.XmlChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

/**
 *
 * A merger that either delegates to [XMLMerger] or
 * [GeoJsonMerger] depending on the types of the chunks to merge.
 *
 * For the time being the merger can only merge chunks of the same type.
 * In the future, it may create an archive (e.g. a ZIP or a TAR file) containing
 * chunks of mixed types.
 * @param optimistic `true` if chunks should be merged optimistically
 * without prior initialization
 * @author Michel Kraemer
 */
class MultiMerger(private val optimistic: Boolean) : Merger<ChunkMeta> {
  private var xmlMerger: XMLMerger? = null
  private var geoJsonMerger: GeoJsonMerger? = null

  private fun ensureMerger(chunkMetadata: ChunkMeta) {
    when (chunkMetadata) {
      is XmlChunkMeta -> if (xmlMerger == null) {
        if (geoJsonMerger != null) {
          throw IllegalStateException("Cannot merge XML chunk into a GeoJSON document.")
        }
        xmlMerger = XMLMerger(optimistic)
      }

      is GeoJsonChunkMeta -> if (geoJsonMerger == null) {
        if (xmlMerger != null) {
          throw IllegalStateException("Cannot merge GeoJSON chunk into an XML document.")
        }
        geoJsonMerger = GeoJsonMerger(optimistic)
      }

      else -> throw IllegalStateException("Cannot merge chunk of type ${chunkMetadata.mimeType}")
    }
  }

  override fun init(chunkMetadata: ChunkMeta) {
    ensureMerger(chunkMetadata)

    when (chunkMetadata) {
      is XmlChunkMeta -> {
        xmlMerger!!.init(chunkMetadata)
      }
      is GeoJsonChunkMeta -> {
        geoJsonMerger!!.init(chunkMetadata)
      }
      else -> throw IllegalArgumentException()  // unreachable
    }
  }

  override suspend fun merge(
    chunk: Buffer, chunkMetadata: ChunkMeta,
    outputStream: WriteStream<Buffer>
  ) {
    ensureMerger(chunkMetadata)

    when (chunkMetadata) {
      is XmlChunkMeta -> {
        xmlMerger!!.merge(chunk, chunkMetadata, outputStream)
      }
      is GeoJsonChunkMeta -> {
        geoJsonMerger!!.merge(chunk, chunkMetadata, outputStream)
      }
      else -> throw IllegalArgumentException()  // unreachable
    }
  }

  override fun finish(outputStream: WriteStream<Buffer>) {
    if (xmlMerger != null) {
      xmlMerger!!.finish(outputStream)
    }
    if (geoJsonMerger != null) {
      geoJsonMerger!!.finish(outputStream)
    }
  }
}
