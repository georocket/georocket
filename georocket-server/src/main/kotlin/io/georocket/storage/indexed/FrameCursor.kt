package io.georocket.storage.indexed

import io.georocket.constants.AddressConstants
import io.georocket.storage.ChunkMeta
import io.georocket.storage.CursorInfo
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.StoreCursor
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.MimeTypeUtils
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait

/**
 * A cursor to run over a subset of data.
 * @author Andrej Sajenko
 * @author Michel Kraemer
 */
class FrameCursor(private val vertx: Vertx, private val search: String? = null,
    private val path: String? = null, private val size: Int = 0,
    private var scrollId: String? = null) : StoreCursor {
  /**
   * The current read position in [ids] and [metas]
   */
  private var pos = -1

  /**
   * The total number of items the store has to offer.
   * If [size] > totalHits then this frame cursor will
   * never load all items of the store.
   */
  private var totalHits: Long = 0

  /**
   * The chunk IDs retrieved in the last batch
   */
  private val ids = mutableListOf<String>()

  /**
   * Chunk metadata retrieved in the last batch
   */
  private val metas = mutableListOf<ChunkMeta>()

  /**
   * Starts this cursor
   */
  suspend fun start(): FrameCursor {
    val queryMsg = JsonObject()
    if (scrollId != null) {
      queryMsg.put("scrollId", scrollId)
    } else {
      queryMsg
          .put("size", size)
          .put("search", search)
      if (path != null) {
        queryMsg.put("path", path)
      }
    }

    val response = vertx.eventBus().requestAwait<JsonObject>(
        AddressConstants.INDEXER_QUERY, queryMsg)
    val body = response.body()

    totalHits = body.getLong("totalHits")
    scrollId = body.getString("scrollId")
    val hits = body.getJsonArray("hits")
    ids.clear()
    metas.clear()
    for (o in hits) {
      val hit = o as JsonObject
      ids.add(hit.getString("id"))
      metas.add(createChunkMeta(hit))
    }

    return this
  }

  override suspend fun hasNext(): Boolean {
    return pos + 1 < metas.size
  }

  override suspend fun next(): ChunkMeta {
    ++pos
    return if (pos >= metas.size) {
      throw IndexOutOfBoundsException("Cursor out of bounds")
    } else {
      metas[pos]
    }
  }

  override val chunkPath: String get() {
    check(pos >= 0) { "You have to call next() first" }
    return ids[pos]
  }

  override val info: CursorInfo get() {
    return CursorInfo(scrollId, totalHits, metas.size)
  }

  /**
   * Create a [XMLChunkMeta] object from a given [hit] object
   */
  private fun createChunkMeta(hit: JsonObject): ChunkMeta {
    val mimeType = hit.getString("mimeType", XMLChunkMeta.MIME_TYPE)
    if (MimeTypeUtils.belongsTo(mimeType, "application", "xml") ||
        MimeTypeUtils.belongsTo(mimeType, "text", "xml")) {
      return XMLChunkMeta(hit)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "geo+json")) {
      return GeoJsonChunkMeta(hit)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "json")) {
      return JsonChunkMeta(hit)
    }
    return ChunkMeta(hit)
  }
}
