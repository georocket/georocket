package io.georocket.storage.file

import io.georocket.coVerify
import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.file.existsAwait
import io.vertx.kotlin.core.file.mkdirsAwait
import io.vertx.kotlin.core.file.readDirAwait
import io.vertx.kotlin.core.file.readFileAwait
import io.vertx.kotlin.core.file.writeFileAwait
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * Test [FileStore]
 * @author Andrej Sajenko
 * @author Michel Kraemer
 */
class FileStoreTest : StorageTest() {
  private lateinit var fileStoreRoot: String
  private lateinit var fileDestination: String

  /**
   * Set up test dependencies.
   */
  @BeforeEach
  fun setUp(@TempDir tempFolder: Path) {
    fileStoreRoot = tempFolder.toFile().absolutePath
    fileDestination = PathUtils.join(fileStoreRoot, "file")
  }

  override suspend fun createStore(vertx: Vertx): Store {
    return FileStore(vertx, fileStoreRoot)
  }

  override suspend fun prepareData(ctx: VertxTestContext, vertx: Vertx, path: String?): String {
    val destinationFolder = if (path == null || path.isEmpty())
      fileDestination else PathUtils.join(fileDestination, path)
    val filePath = PathUtils.join(destinationFolder, ID)
    val fs = vertx.fileSystem()
    fs.mkdirsAwait(destinationFolder)
    fs.writeFileAwait(filePath, Buffer.buffer(CHUNK_CONTENT))
    return filePath.toString().replace("$fileDestination/", "")
  }

  override suspend fun validateAfterStoreAdd(ctx: VertxTestContext, vertx: Vertx,
      path: String?) {
    val fs = vertx.fileSystem()
    val destinationFolder = if (path == null || path.isEmpty())
      fileDestination else PathUtils.join(fileDestination, path)

    ctx.coVerify {
      assertThat(fs.existsAwait(destinationFolder)).isTrue
      val files = fs.readDirAwait(destinationFolder)
      assertThat(files).hasSize(1)
      val file = files[0]
      val contents = fs.readFileAwait(file)
      assertThat(contents.toString()).isEqualTo(CHUNK_CONTENT)
    }
  }

  override suspend fun validateAfterStoreDelete(ctx: VertxTestContext,
      vertx: Vertx, path: String) {
    val fs = vertx.fileSystem()
    ctx.coVerify {
      assertThat(fs.existsAwait(path)).isFalse()
    }
  }
}
