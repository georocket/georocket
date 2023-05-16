package io.georocket.storage.postgresql

import io.georocket.index.postgresql.PostgreSQLClient
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.pgclient.PgConnectOptions
import io.vertx.pgclient.PgPool
import io.vertx.sqlclient.PoolOptions
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.AfterEach
import org.testcontainers.containers.JdbcDatabaseContainer

/**
 * Common code for all tests that need a PostgreSQL database
 * @author MicheL Kraemer
 */
interface PostgreSQLTest {
  companion object {
    const val TAG = "14.1"
  }

  val postgresql: JdbcDatabaseContainer<*>

  val url: String get() = postgresql.jdbcUrl.let {
    if (it.startsWith("jdbc:")) it.substring(5) else it }

  suspend fun <T> withClient(vertx: Vertx, block: suspend (PgPool) -> T): T {
    val connectOptions = PgConnectOptions.fromUri(url)
      .setUser(postgresql.username)
      .setPassword(postgresql.password)

    val poolOptions = PoolOptions()
      .setMaxSize(5)

    val client = PgPool.pool(vertx, connectOptions, poolOptions)

    try {
      return block(client)
    } finally {
      client.close()
    }
  }

  /**
   * Clear database after each test
   */
  @AfterEach
  fun tearDownDatabase(vertx: Vertx, ctx: VertxTestContext) {
    CoroutineScope(vertx.dispatcher()).launch {
      withClient(vertx) { client ->
        deleteFromTables(client)
      }
      ctx.completeNow()
    }

    // make sure migrations will run for the next unit test
    PostgreSQLClient.migratedDatabases.clear()
  }

  /**
   * Will be called after each test to clean up all tables
   */
  suspend fun deleteFromTables(client: PgPool)
}
