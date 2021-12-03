package io.georocket.storage

import io.georocket.coVerify
import io.georocket.constants.AddressConstants
import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Abstract test implementation for a [Store]
 *
 * This class defines test methods for the store interface and should be
 * used as base class for all concrete Store tests.
 *
 * A concrete store test implement only the data preparation and some
 * validation methods which have access to the storage system.
 *
 * @author Andrej Sajenko
 * @author Michel Kraemer
 */
@ExtendWith(VertxExtension::class)
abstract class StorageTest {
  companion object {
    /**
     * Test data: tempFolder name which is used to call the test with a folder
     */
    protected const val TEST_FOLDER = "testFolder"

    /**
     * Test data: content of a chunk
     */
    @JvmStatic
    val CHUNK_CONTENT = "<b>This is a chunk content</b>"

    /**
     * Test data: search for a Store (value is irrelevant for the test, because
     * this test do not use the Indexer)
     */
    protected const val SEARCH = "irrelevant but necessary value"

    /**
     * Test data: fallback CRS for chunk indexing
     */
    protected const val FALLBACK_CRS_STRING = "EPSG:25832"

    /**
     * Test data: the import id of a file import
     */
    protected const val IMPORT_ID = "Af023dasd3"

    /**
     * Test data: the timestamp for an import
     */
    protected val TIMESTAMP = System.currentTimeMillis()

    /**
     * Test data: a sample tag list for an Store::add method
     */
    protected val TAGS = listOf("a", "b", "c")

    /**
     * Test data: a sample property map for an Store::add method
     */
    protected val PROPERTIES: Map<String, Any> = mapOf("k1" to "v1", "k2" to "v2", "k3" to "v3")

    /**
     * Test data: a randomly generated id for all tests.
     */
    @JvmStatic
    val ID = UniqueID.next()

    /**
     * Test data: the parents of one hit
     */
    protected val PARENTS = JsonArray()

    /**
     * Test data: start of a hit
     */
    protected const val START = 0

    /**
     * Test data: end of a hit
     */
    protected const val END = 5

    /**
     * Test data: Total amount of hits
     */
    protected const val TOTAL_HITS = 1L

    /**
     * Test data: scroll id
     */
    protected const val SCROLL_ID = "0"

    /**
     * Create a JsonObject from an optional [pathPrefix] to simulate a reply
     * from an indexer
     */
    protected fun createIndexerQueryReply(pathPrefix: String?): JsonObject {
      val path = if (pathPrefix != null && pathPrefix.isNotBlank()) {
        PathUtils.join(pathPrefix, ID)
      } else {
        ID
      }

      return json {
        obj(
            "totalHits" to TOTAL_HITS,
            "scrollId" to SCROLL_ID,
            "hits" to array(
                obj(
                    "parents" to PARENTS,
                    "start" to START,
                    "end" to END,
                    "id" to path
                )
            )
        )
      }
    }
  }

  /**
   * Create the store under test
   */
  protected abstract suspend fun createStore(vertx: Vertx): Store

  /**
   * Prepare test data for (every) test. Will be called during every test.
   *
   * Heads up: use the protected attributes in the companion object as test data
   *
   * @param path the path for the data (may be null).
   */
  protected abstract suspend fun prepareData(ctx: VertxTestContext,
      vertx: Vertx, path: String?): String

  /**
   * Validate the add method. Will be called after the store added data.
   *
   * Heads up: look on the protected attributes of this class to know which
   * data were used for the store add method. These will be used for the
   * [Store.add] method.
   *
   * @param path The path where the data was created (may be `null` if not used
   * for [prepareData])
   */
  protected abstract suspend fun validateAfterStoreAdd(ctx: VertxTestContext,
      vertx: Vertx, path: String?)

  /**
   * Validate the delete method of a test. Will be called after the store
   * deleted data.
   *
   * Heads up: look on the protected attributes and your [prepareData]
   * implementation to know which data you have deleted with the
   * [Store.delete] method.
   *
   * @param path The path where the data were created
   */
  protected abstract suspend fun validateAfterStoreDelete(ctx: VertxTestContext,
      vertx: Vertx, path: String)

  private fun mockIndexerQuery(ctx: VertxTestContext, vertx: Vertx, path: String?): Promise<Unit> {
    val result = Promise.promise<Unit>()
    vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_QUERY).handler { request ->
      val msg = request.body()
      ctx.verify {
        assertThat(msg.map).containsKey("size")
        assertThat(msg.map).containsKey("search")
        val indexSearch = msg.getString("search")
        assertThat(indexSearch).isEqualTo(SEARCH)
        request.reply(createIndexerQueryReply(path))
        result.complete()
      }
    }
    return result
  }

  /**
   * Call [testAdd] with `null` as path
   */
  @Test
  fun testAddWithoutSubfolder(ctx: VertxTestContext, vertx: Vertx) {
    testAdd(ctx, vertx, null)
  }

  /**
   * Call [testAdd] with a path
   */
  @Test
  fun testAddWithSubfolder(ctx: VertxTestContext, vertx: Vertx) {
    testAdd(ctx, vertx, TEST_FOLDER)
  }

  /**
   * Call [testDelete] with `null` as path.
   */
  @Test
  fun testDeleteWithoutSubfolder(ctx: VertxTestContext, vertx: Vertx) {
    testDelete(ctx, vertx, null)
  }

  /**
   * Call [testDelete] with a path.
   */
  @Test
  fun testDeleteWithSubfolder(ctx: VertxTestContext, vertx: Vertx) {
    testDelete(ctx, vertx, TEST_FOLDER)
  }

  /**
   * Call [testGet] with null as path.
   */
  @Test
  fun testGetWithoutSubfolder(ctx: VertxTestContext, vertx: Vertx) {
    testGet(ctx, vertx, null)
  }

  /**
   * Call [testGet] with a path.
   */
  @Test
  fun testGetWithSubfolder(ctx: VertxTestContext, vertx: Vertx) {
    testGet(ctx, vertx, TEST_FOLDER)
  }

  /**
   * Get chunk by ID
   */
  @Test
  fun testGetOneWithoutFolder(ctx: VertxTestContext, vertx: Vertx) {
    GlobalScope.launch(vertx.dispatcher()) {
      prepareData(ctx, vertx, null)

      val store = createStore(vertx)
      val chunk = store.getOne(ID).toString()
      ctx.verify {
        assertThat(chunk).isEqualTo(CHUNK_CONTENT)
      }

      ctx.completeNow()
    }
  }

  /**
   * Add test data and compare the data with the stored one
   */
  private fun testAdd(ctx: VertxTestContext, vertx: Vertx, path: String?) {
    GlobalScope.launch(vertx.dispatcher()) {
      val store = createStore(vertx)

      // register query
      vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_QUERY).handler {
        ctx.failNow(IllegalStateException("Indexer should not be notified for " +
            "a query event after Store::add was called!"))
      }

      val indexMeta = IndexMeta(IMPORT_ID, ID, TIMESTAMP, TAGS, PROPERTIES, FALLBACK_CRS_STRING)

      ctx.coVerify {
        val p = store.makePath(indexMeta, path ?: "/")
        store.add(Buffer.buffer(CHUNK_CONTENT), p)
        validateAfterStoreAdd(ctx, vertx, path)
      }
      ctx.completeNow()
    }
  }

  /**
   * Add test data and try to delete them with the [Store.delete] method,
   * then check the storage for any data
   */
  private fun testDelete(ctx: VertxTestContext, vertx: Vertx, path: String?) {
    GlobalScope.launch(vertx.dispatcher()) {
      val resultPath = prepareData(ctx, vertx, path)

      val store = createStore(vertx)

      // register query
      val queryPromise = mockIndexerQuery(ctx, vertx, path)
      store.delete(listOf(resultPath))

      validateAfterStoreDelete(ctx, vertx, resultPath)

      queryPromise.future().await()

      ctx.completeNow()
    }
  }

  /**
   * Add test data with meta data and try to retrieve them with the
   * [Store.get] method
   */
  private fun testGet(ctx: VertxTestContext, vertx: Vertx, path: String?) {
    GlobalScope.launch(vertx.dispatcher()) {
      // register query
      val queryPromise = mockIndexerQuery(ctx, vertx, path)

      val resultPath = prepareData(ctx, vertx, path)

      val store = createStore(vertx)
      val cursor = store.get(SEARCH, resultPath)

      ctx.coVerify {
        assertThat(cursor.hasNext()).isTrue
        val meta = cursor.next()
        assertThat(meta.getEnd()).isEqualTo(END)
        assertThat(meta.getStart()).isEqualTo(START)
        val fileName = cursor.chunkPath
        assertThat(fileName).isEqualTo(PathUtils.join(path, ID))
      }

      queryPromise.future().await()

      ctx.completeNow()
    }
  }
}
