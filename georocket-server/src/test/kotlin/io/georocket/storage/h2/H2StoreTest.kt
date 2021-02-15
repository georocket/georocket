package io.georocket.storage.h2

import io.georocket.storage.StorageTest
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

/**
 * Test [H2Store]
 * @author Michel Kraemer
 */
class H2StoreTest : StorageTest() {
  private lateinit var path: String
  private lateinit var store: H2Store

  /**
   * Set up the test
   */
  @BeforeEach
  fun setUp(vertx: Vertx, @TempDir tempFolder: Path) {
    path = File(tempFolder.toFile(), "h2").absolutePath
    store = H2Store(vertx, path)
  }

  /**
   * Release test resources
   */
  @AfterEach
  fun tearDown() {
    store.close()
  }

  override fun createStore(vertx: Vertx): H2Store {
    return store
  }

  override suspend fun prepareData(ctx: VertxTestContext, vertx: Vertx,
      path: String?): String {
    val p = PathUtils.join(path, ID)
    store.map[p] = CHUNK_CONTENT
    return p
  }

  override suspend fun validateAfterStoreAdd(ctx: VertxTestContext,
      vertx: Vertx, path: String?) {
    ctx.verify {
      assertThat(store.map).hasSize(1)
    }
  }

  override suspend fun validateAfterStoreDelete(ctx: VertxTestContext,
      vertx: Vertx, path: String) {
    ctx.verify {
      assertThat(store.map).hasSize(0)
    }
  }
}
