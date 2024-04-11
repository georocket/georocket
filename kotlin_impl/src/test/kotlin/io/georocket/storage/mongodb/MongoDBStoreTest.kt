package io.georocket.storage.mongodb

import com.mongodb.reactivestreams.client.MongoClients
import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.georocket.util.insertOneAwait
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.assertj.core.api.Assertions.assertThat
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll

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
  fun tearDown(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      MongoClients.create(mongoConnector.connectionString).use { client ->
        val db = client.getDatabase(mongoConnector.connectionString.database)
        db.drop().awaitFirstOrNull()
        ctx.completeNow()
      }
    }
  }

  override suspend fun createStore(vertx: Vertx): Store {
    return MongoDBStore.create(vertx, mongoConnector.connectionString.toString())
  }

  override suspend fun prepareData(ctx: VertxTestContext, vertx: Vertx,
      path: String?): String {
    val filename = PathUtils.join(path, ID)
    return MongoClients.create(mongoConnector.connectionString).use { client ->
      val db = client.getDatabase(mongoConnector.connectionString.database)
      val chunks = db.getCollection("chunks", BsonDocument::class.java)
      val doc = BsonDocument()
      doc["filename"] = BsonString(filename)
      doc["n"] = BsonInt32(0)
      doc["data"] = BsonBinary(CHUNK_CONTENT.toByteArray())
      chunks.insertOneAwait(doc)
      filename
    }
  }

  override suspend fun validateAfterStoreAdd(ctx: VertxTestContext, vertx: Vertx,
      path: String?) {
    MongoClients.create(mongoConnector.connectionString).use { client ->
      val db = client.getDatabase(mongoConnector.connectionString.database)
      val chunks = db.getCollection("chunks", BsonDocument::class.java)
      val files = chunks.find().asFlow().toList()
      assertThat(files).hasSize(1)
      val file = files.first()
      val contents = String(file.getBinary("data").data)
      assertThat(contents).isEqualTo(CHUNK_CONTENT)
    }
  }

  override suspend fun validateAfterStoreDelete(ctx: VertxTestContext,
      vertx: Vertx, path: String) {
    MongoClients.create(mongoConnector.connectionString).use { client ->
      val db = client.getDatabase(mongoConnector.connectionString.database)
      val chunks = db.getCollection("chunks")
      val count = chunks.countDocuments().awaitSingle()
      assertThat(count).isEqualTo(0)
    }
  }
}
