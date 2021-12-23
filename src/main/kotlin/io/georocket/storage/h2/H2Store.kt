package io.georocket.storage.h2

import io.georocket.constants.ConfigConstants
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import java.io.FileNotFoundException

/**
 * Stores chunks in a H2 database
 * @author Michel Kraemer
 */
class H2Store(vertx: Vertx, path: String? = null) : Store {
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

  override suspend fun add(chunk: Buffer, path: String) {
    map[path] = chunk
  }

  override suspend fun delete(paths: Flow<String>): Long {
    var result = 0L
    paths.collect {
      val old = map.remove(it)
      if (old != null) {
        result++
      }
    }
    return result
  }
}
