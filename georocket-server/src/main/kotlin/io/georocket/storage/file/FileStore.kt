package io.georocket.storage.file

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkReadStream
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
import io.vertx.kotlin.core.file.openOptionsOf
import io.vertx.kotlin.core.file.propsAwait
import io.vertx.kotlin.core.file.writeAwait
import java.io.FileNotFoundException

/**
 * Stores chunks on the file system
 * @author Michel Kraemer
 */
class FileStore(private val vertx: Vertx) : IndexedStore(vertx) {
  /**
   * The folder where the chunks should be saved
   */
  private val root: String

  init {
    val storagePath = vertx.orCreateContext.config().getString(
        ConfigConstants.STORAGE_FILE_PATH)
        ?: throw IllegalArgumentException("Missing configuration item \"" +
            ConfigConstants.STORAGE_FILE_PATH + "\"")
    this.root = PathUtils.join(storagePath, "file")
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
    val filepath = PathUtils.join(path, filename)

    // open new file
    val f = fs.openAwait(filepath, OpenOptions())

    // write contents to file
    val buf = Buffer.buffer(chunk)
    f.writeAwait(buf)
    f.closeAwait()

    return filepath
  }

  override suspend fun getOne(path: String): ChunkReadStream {
    val absolutePath = PathUtils.join(root, path)

    // check if chunk exists
    val fs = vertx.fileSystem()
    if (!fs.existsAwait(absolutePath)) {
      throw FileNotFoundException("Could not find chunk: $path")
    }

    // get chunk's size
    val props = fs.propsAwait(absolutePath)
    val size = props.size()

    // open chunk
    val f = fs.openAwait(absolutePath, openOptionsOf(create = false, write = false))

    return FileChunkReadStream(size, f)
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
