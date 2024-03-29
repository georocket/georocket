package io.georocket.storage.file

import io.georocket.constants.ConfigConstants
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.OpenOptions
import io.vertx.kotlin.coroutines.await
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

  override suspend fun add(chunk: Buffer, path: String) {
    val filepath = PathUtils.join(root, path)
    val parent = filepath.substring(0, filepath.lastIndexOf('/'))

    // create storage folder
    val fs = vertx.fileSystem()
    fs.mkdirs(parent).await()

    // open new file
    val f = fs.open(filepath, OpenOptions()).await()

    // write contents to file
    f.write(chunk).await()
    f.close().await()
  }

  override suspend fun getOne(path: String): Buffer {
    val absolutePath = PathUtils.join(root, path)

    // check if chunk exists
    val fs = vertx.fileSystem()
    if (!fs.exists(absolutePath).await()) {
      throw FileNotFoundException("Could not find chunk: $path")
    }

    return fs.readFile(absolutePath).await()
  }

  override suspend fun delete(paths: Collection<String>): Long {
    var result = 0L
    val fs = vertx.fileSystem()
    for (path in paths) {
      val absolutePath = PathUtils.join(root, path)
      if (fs.exists(absolutePath).await()) {
        fs.delete(absolutePath).await()
        result++
      }
    }
    return result
  }
}
