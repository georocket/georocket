package io.georocket.storage.mongodb

import com.mongodb.ConnectionString
import de.flapdoodle.embed.mongo.MongodExecutable
import de.flapdoodle.embed.mongo.MongodProcess
import com.mongodb.ServerAddress
import de.flapdoodle.embed.mongo.MongodStarter
import io.georocket.storage.mongodb.MongoDBTestConnector
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import io.georocket.NetUtils

/**
 * Starts and stops a MongoDB instance
 *
 * Don't forget to call [stop] if you don't need it anymore!
 *
 * @author Michel Kraemer
 */
class MongoDBTestConnector {
  companion object {
    const val MONGODB_DBNAME = "testdb"

    private val starter = MongodStarter.getDefaultInstance()
  }

  private val mongodExe: MongodExecutable
  private val mongod: MongodProcess

  /**
   * The address of the MongoDB instance
   */
  private val serverAddress = ServerAddress("localhost", NetUtils.findPort())
  val connectionString = ConnectionString("""mongodb://${serverAddress}/testdb""")

  /**
   * Start MongoDB instance
   */
  init {
    mongodExe = starter.prepare(MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .net(Net(serverAddress.port, Network.localhostIsIPv6()))
        .build())
    mongod = mongodExe.start()
  }

  /**
   * Stop MongoDB instance
   */
  fun stop() {
    mongod.stop()
    mongodExe.stop()
  }
}
