package io.georocket.index

import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.Caffeine
import io.georocket.storage.ChunkMeta
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.MimeTypeUtils
import io.vertx.core.json.JsonObject
import java.time.Duration

/**
 * Abstract base class for all [Index] implementations
 * @author Michel Kraemer
 */
abstract class AbstractIndex {
  companion object {
    /**
     * A cache used when importing chunk metadata, so that the same objects are
     * not inserted over and over again
     */
    @JvmStatic
    protected val addedChunkMetaCache: AsyncCache<ChunkMeta, String> = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(Duration.ofDays(1))
      .buildAsync()

    /**
     * A cache used when loading chunk metadata by ID
     */
    @JvmStatic
    protected val loadedChunkMetaCache: AsyncCache<String, ChunkMeta> = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(Duration.ofDays(1))
      .buildAsync()
  }

  /**
   * Extract a path string and a [ChunkMeta] object from a given [cm] object
   */
  protected fun createChunkMeta(cm: JsonObject): ChunkMeta {
    val mimeType = cm.getString("mimeType", XMLChunkMeta.MIME_TYPE)
    return if (MimeTypeUtils.belongsTo(mimeType, "application", "xml") ||
      MimeTypeUtils.belongsTo(mimeType, "text", "xml")) {
      XMLChunkMeta(cm)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "geo+json")) {
      GeoJsonChunkMeta(cm)
    } else if (MimeTypeUtils.belongsTo(mimeType, "application", "json")) {
      JsonChunkMeta(cm)
    } else {
      ChunkMeta(cm)
    }
  }
}
