package io.georocket.storage.mongodb

import com.mongodb.ConnectionString
import com.mongodb.ServerAddress
import de.flapdoodle.embed.mongo.MongodExecutable
import de.flapdoodle.embed.mongo.MongodProcess
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.MongodConfig
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
    mongodExe = starter.prepare(MongodConfig.builder()
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
