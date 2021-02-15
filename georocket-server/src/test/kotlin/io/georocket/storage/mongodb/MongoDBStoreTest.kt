package io.georocket.storage.mongodb

import com.mongodb.reactivestreams.client.MongoClients
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets
import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitSingle
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import reactor.core.publisher.Mono
import java.nio.ByteBuffer

/**
 * Test [MongoDBStore]
 * @author Andrej Sajenko
 */
class MongoDBStoreTest : StorageTest() {
  companion object {
    private lateinit var mongoConnector: MongoDBTestConnector

    /**
     * Set up test dependencies
     */
    @BeforeAll
    @JvmStatic
    fun setUpClass() {
      mongoConnector = MongoDBTestConnector()
    }

    /**
     * Uninitialize tests
     */
    @AfterAll
    @JvmStatic
    fun tearDownClass() {
      mongoConnector.stop()
    }
  }

  /**
   * Uninitialize tests
   */
  @AfterEach
  fun tearDown() {
    return MongoClients.create(mongoConnector.connectionString).use { client ->
      val db = client.getDatabase(mongoConnector.connectionString.database)
      db.drop()
    }
  }

  override fun createStore(vertx: Vertx): Store {
    return MongoDBStore(vertx, mongoConnector.connectionString.toString())
  }

  override suspend fun prepareData(ctx: VertxTestContext, vertx: Vertx,
      path: String?): String {
    val filename = PathUtils.join(path, ID)
    return MongoClients.create(mongoConnector.connectionString).use { client ->
      val db = client.getDatabase(mongoConnector.connectionString.database)
      val gridFS = GridFSBuckets.create(db)
      val contents = ByteBuffer.wrap(CHUNK_CONTENT.toByteArray())
      gridFS.uploadFromPublisher(filename, Mono.just(contents)).awaitSingle()
      filename
    }
  }

  override suspend fun validateAfterStoreAdd(ctx: VertxTestContext, vertx: Vertx,
      path: String?) {
    MongoClients.create(mongoConnector.connectionString).use { client ->
      val db = client.getDatabase(mongoConnector.connectionString.database)
      val gridFS = GridFSBuckets.create(db)
      val files = gridFS.find()
      val file = files.awaitFirst()
      val publisher = gridFS.downloadToPublisher(file.filename)
      val bytebuf = publisher.awaitSingle()
      val contents = String(bytebuf.array())
      assertThat(contents).isEqualTo(CHUNK_CONTENT)
    }
  }

  override suspend fun validateAfterStoreDelete(ctx: VertxTestContext,
      vertx: Vertx, path: String) {
    MongoClients.create(mongoConnector.connectionString).use { client ->
      val db = client.getDatabase(mongoConnector.connectionString.database)
      val filesCollection = db.getCollection("fs.files")
      val count = filesCollection.countDocuments().awaitSingle()
      assertThat(count).isEqualTo(0)
    }
  }
}
