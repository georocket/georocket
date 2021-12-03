package io.georocket.storage.h2

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkMeta
import io.georocket.storage.IndexMeta
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import java.io.FileNotFoundException

/**
 * Stores chunks in a H2 database
 * @author Michel Kraemer
 */
class H2Store(vertx: Vertx, path: String? = null) : IndexedStore(vertx) {
  internal val map: SharedMVMap

  init {
    val config = vertx.orCreateContext.config()

    val actualPath = path ?: config.getString(ConfigConstants.STORAGE_H2_PATH) ?:
        throw IllegalStateException("Missing configuration item \"" +
            ConfigConstants.STORAGE_H2_PATH + "\"")

    val compress = config.getBoolean(ConfigConstants.STORAGE_H2_COMPRESS, false)
    val mapName = config.getString(ConfigConstants.STORAGE_H2_MAP_NAME, "georocket")

    map = SharedMVMap.create(actualPath, mapName, compress)
  }

  override fun close() {
    map.close()
  }

  override suspend fun getOne(path: String): Buffer {
    val finalPath = PathUtils.normalize(path)
    return map[finalPath]
        ?: throw FileNotFoundException("Could not find chunk: $finalPath")
  }

  override suspend fun add(chunk: Buffer, chunkMetadata: ChunkMeta,
      indexMetadata: IndexMeta, layer: String): String {
    val path = layer.ifEmpty { "/" }
    val filename = PathUtils.join(path, indexMetadata.correlationId + UniqueID.next())
    map[filename] = chunk
    return filename
  }

  override suspend fun delete(paths: Collection<String>) {
    paths.forEach { map.remove(it) }
  }
}
