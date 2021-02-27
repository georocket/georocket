package io.georocket.storage.file

import io.georocket.constants.ConfigConstants
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.OpenOptions
import io.vertx.kotlin.core.file.closeAwait
import io.vertx.kotlin.core.file.deleteAwait
import io.vertx.kotlin.core.file.existsAwait
import io.vertx.kotlin.core.file.mkdirsAwait
import io.vertx.kotlin.core.file.openAwait
import io.vertx.kotlin.core.file.readFileAwait
import io.vertx.kotlin.core.file.writeAwait
import java.io.FileNotFoundException

/**
 * Stores chunks on the file system
 * @author Michel Kraemer
 */
class FileStore(private val vertx: Vertx, storagePath: String? = null) : IndexedStore(vertx) {
  /**
   * The folder where the chunks should be saved
   */
  private val root: String

  init {
    val actualStoragePath = storagePath ?: vertx.orCreateContext.config().getString(
        ConfigConstants.STORAGE_FILE_PATH) ?: throw IllegalArgumentException(
        """Missing configuration item "${ConfigConstants.STORAGE_FILE_PATH}"""")
    root = PathUtils.join(actualStoragePath, "file")
  }

  override suspend fun doAddChunk(chunk: String, layer: String,
      correlationId: String): String {
    val path = if (layer.isEmpty()) "/" else layer

    val dir = PathUtils.join(root, path)

    // create storage folder
    val fs = vertx.fileSystem()
    fs.mkdirsAwait(dir)

    // generate new file name
    val filename = correlationId + UniqueID.next()
    val filepath = PathUtils.join(dir, filename)

    // open new file
    val f = fs.openAwait(filepath, OpenOptions())

    // write contents to file
    val buf = Buffer.buffer(chunk)
    f.writeAwait(buf)
    f.closeAwait()

    return PathUtils.join(path, filename)
  }

  override suspend fun getOne(path: String): Buffer {
    val absolutePath = PathUtils.join(root, path)

    // check if chunk exists
    val fs = vertx.fileSystem()
    if (!fs.existsAwait(absolutePath)) {
      throw FileNotFoundException("Could not find chunk: $path")
    }

    return fs.readFileAwait(absolutePath)
  }

  override suspend fun doDeleteChunks(paths: Iterable<String>) {
    val fs = vertx.fileSystem()
    for (path in paths) {
      val absolutePath = PathUtils.join(root, path)
      if (fs.existsAwait(absolutePath)) {
        fs.deleteAwait(absolutePath)
      }
    }
  }
}
