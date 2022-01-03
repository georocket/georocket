package io.georocket.index.postgresql

import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.await
import io.vertx.pgclient.PgConnectOptions
import io.vertx.pgclient.PgPool
import io.vertx.pgclient.impl.PgConnectionUriParser
import io.vertx.sqlclient.PoolOptions
import io.vertx.sqlclient.SqlConnection
import org.flywaydb.core.Flyway

open class PostgreSQLClient(vertx: Vertx, url: String, username: String, password: String) {
  companion object {
    /**
     * Table and column names
     */
    @JvmStatic protected val ID = "id"
    @JvmStatic protected val DATA = "data"

    /**
     * Holds information about a database that has already been migrated
     */
    internal data class MigratedDatabase(val url: String, val user: String, val password: String)

    /**
     * Keeps all databases that have already been migrated
     */
    internal val migratedDatabases = mutableSetOf<MigratedDatabase>()

    /**
     * Perform database migrations
     * @param url the JDBC url to the database
     * @param user the username
     * @param password the password
     */
    @Synchronized
    private fun migrate(url: String, user: String, password: String) {
      val md = MigratedDatabase(url, user, password)
      if (!migratedDatabases.contains(md)) {
        val flyway = Flyway.configure()
          .dataSource(url, user, password)
          .load()
        flyway.migrate()
        migratedDatabases.add(md)
      }
    }
  }

  val client: PgPool

  init {
    migrate(url, username, password)

    val uri = if (url.startsWith("jdbc:")) url.substring(5) else url
    val parsedConfiguration = PgConnectionUriParser.parse(uri)
    val connectOptions = PgConnectOptions(parsedConfiguration)
      .setUser(username)
      .setPassword(password)
    if (!parsedConfiguration.containsKey("search_path") &&
      parsedConfiguration.containsKey("currentschema")) {
      connectOptions.addProperty("search_path",
        parsedConfiguration.getString("currentschema"))
    }

    val poolOptions = PoolOptions()
      .setMaxSize(5)

    client = PgPool.pool(vertx, connectOptions, poolOptions)
  }

  open suspend fun close() {
    client.close()
  }

  suspend fun <T> withConnection(block: suspend (SqlConnection) -> T): T {
    val connection = client.connection.await()
    try {
      return block(connection)
    } finally {
      connection.close()
    }
  }
}
