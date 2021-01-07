package io.georocket.storage.h2

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import org.h2.mvstore.MVStore
import java.io.FileNotFoundException

/**
 * Stores chunks in a H2 database
 * @author Michel Kraemer
 */
class H2Store(vertx: Vertx) : IndexedStore(vertx) {
  private val mvStoreHolder: SharedMVStoreHolder
  private val mvstore: MVStore get() = mvStoreHolder.mvStore

  /**
   * The name of the MVMap within the H2 database.
   */
  private val mapName: String

  init {
    val config = vertx.orCreateContext.config()

    val path = config.getString(ConfigConstants.STORAGE_H2_PATH) ?:
        throw IllegalStateException("Missing configuration item \"" +
            ConfigConstants.STORAGE_H2_PATH + "\"")

    val compress = config.getBoolean(ConfigConstants.STORAGE_H2_COMPRESS, false)
    mapName = config.getString(ConfigConstants.STORAGE_H2_MAP_NAME, "georocket")

    mvStoreHolder = SharedMVStoreHolder(path, compress)
  }

  /**
   * The underlying H2 MVMap
   */
  private val map: MutableMap<String, String> by lazy {
    mvstore.openMap(mapName)
  }

  /**
   * Release all resources and close this store
   */
  fun close() {
    mvStoreHolder.close()
  }

  override suspend fun getOne(path: String): ChunkReadStream {
    val finalPath = PathUtils.normalize(path)
    val chunkStr = map[finalPath]
        ?: throw FileNotFoundException("Could not find chunk: $finalPath")
    val chunk = Buffer.buffer(chunkStr)
    return DelegateChunkReadStream(chunk)
  }

  override suspend fun doAddChunk(chunk: String, layer: String,
      correlationId: String): String {
    val path = if (layer.isEmpty()) "/" else layer
    val filename = PathUtils.join(path, correlationId + UniqueID.next())
    map[filename] = chunk
    return filename
  }

  override suspend fun doDeleteChunks(paths: Iterable<String>) {
    paths.forEach { map.remove(it) }
  }
}
