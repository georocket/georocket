package io.georocket.index.postgresql

import io.georocket.index.Index
import io.georocket.index.IndexTest
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.storage.postgresql.PostgreSQLTest
import io.vertx.core.Vertx
import io.vertx.pgclient.PgPool
import org.testcontainers.containers.PostgreSQLContainerProvider
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Test [PostgreSQLIndex]
 * @author Tobias Dorra
 */
@Testcontainers
class PostgreSQLIndexTest : IndexTest(), PostgreSQLTest {
  companion object {
    @Container
    val postgresqlContainer = PostgreSQLContainerProvider().newInstance(PostgreSQLTest.TAG)!!
  }

  override val postgresql = postgresqlContainer

  override suspend fun createIndex(vertx: Vertx): Index =
    PostgreSQLIndex.create(vertx, postgresql.jdbcUrl, postgresql.username, postgresql.password)

  override suspend fun prepareTestData(vertx: Vertx, docs: List<Index.AddManyParam>) {
    withClient(vertx) { client ->
      deleteFromTables(client)
    }
    val index = createIndex(vertx)
    index.addMany(docs)
    index.close()
  }

  override suspend fun deleteFromTables(client: PgPool) {
    client.query("TRUNCATE TABLE chunkmeta, chunks, documents")
  }
}
