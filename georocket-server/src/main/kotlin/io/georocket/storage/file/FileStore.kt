package io.georocket.storage.file

import io.georocket.constants.ConfigConstants
import io.georocket.storage.IndexMeta
import io.georocket.storage.Store
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
class FileStore(private val vertx: Vertx, storagePath: String? = null) : Store {
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

  override fun makePath(indexMetadata: IndexMeta, layer: String): String {
    val path = layer.ifEmpty { "/" }
    val filename = indexMetadata.correlationId + UniqueID.next()
    return PathUtils.join(path, filename)
  }

  override suspend fun add(chunk: Buffer, path: String) {
    val filepath = PathUtils.join(root, path)
    val parent = filepath.substring(0, filepath.lastIndexOf('/'))

    // create storage folder
    val fs = vertx.fileSystem()
    fs.mkdirsAwait(parent)

    // open new file
    val f = fs.openAwait(filepath, OpenOptions())

    // write contents to file
    f.writeAwait(chunk)
    f.closeAwait()
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

  override suspend fun delete(paths: Collection<String>) {
    val fs = vertx.fileSystem()
    for (path in paths) {
      val absolutePath = PathUtils.join(root, path)
      if (fs.existsAwait(absolutePath)) {
        fs.deleteAwait(absolutePath)
      }
    }
  }
}
