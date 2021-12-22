package io.georocket.storage.postgresql

import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import io.vertx.pgclient.PgPool
import io.vertx.sqlclient.Tuple
import org.assertj.core.api.Assertions.assertThat
import org.testcontainers.containers.PostgreSQLContainerProvider
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Test [PostgreSQLStore]
 * @author Michel Kraemer
 */
@Testcontainers
class PostgreSQLStoreTest : StorageTest(), PostgreSQLTest {
  companion object {
    @Container
    val _postgresql = PostgreSQLContainerProvider().newInstance(PostgreSQLTest.TAG)!!
  }

  override val postgresql = _postgresql

  override suspend fun createStore(vertx: Vertx): Store {
    return PostgreSQLStore.create(vertx, postgresql.jdbcUrl, postgresql.username, postgresql.password)
  }

  override suspend fun prepareData(ctx: VertxTestContext, vertx: Vertx, path: String?): String {
    return withClient(vertx) { client ->
      val filename = PathUtils.join(path, ID)
      val statement = "INSERT INTO chunks (id, data) VALUES ($1, $2)"
      val params = Tuple.of(filename, Buffer.buffer(CHUNK_CONTENT))
      client.preparedQuery(statement).execute(params).await()
      filename
    }
  }

  override suspend fun deleteFromTables(client: PgPool) {
    client.query("DELETE FROM chunks").execute().await()
  }

  override suspend fun validateAfterStoreAdd(ctx: VertxTestContext,
      vertx: Vertx, path: String?) {
    return withClient(vertx) { client ->
      val statement = "SELECT id, data FROM chunks"
      val rs = client.preparedQuery(statement).execute().await()
      val bufs = rs.map { it.getString(0) to it.getBuffer(1) }
      assertThat(bufs).hasSize(1)
      if (path != null) {
        assertThat(bufs[0].first).startsWith(path)
      }
      assertThat(bufs[0].second.toString()).isEqualTo(CHUNK_CONTENT)
    }
  }

  override suspend fun validateAfterStoreDelete(ctx: VertxTestContext,
      vertx: Vertx, path: String) {
    TODO("Not yet implemented")
  }
}
