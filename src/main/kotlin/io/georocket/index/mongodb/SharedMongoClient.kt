package io.georocket.index.mongodb

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import de.flapdoodle.embed.mongo.Command
import de.flapdoodle.embed.mongo.MongodExecutable
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.Defaults
import de.flapdoodle.embed.mongo.config.MongodConfig
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.config.Storage
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.impl.codec.json.JsonObjectCodec
import io.vertx.kotlin.coroutines.await
import org.bson.codecs.BooleanCodec
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DoubleCodec
import org.bson.codecs.IntegerCodec
import org.bson.codecs.LongCodec
import org.bson.codecs.StringCodec
import org.bson.codecs.configuration.CodecRegistries
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class SharedMongoClient(private val key: ConnectionString,
    private val client: MongoClient) : MongoClient by client {
  private var instanceCount = 0

  companion object {
    private val log = LoggerFactory.getLogger(SharedMongoClient::class.java)

    const val DEFAULT_EMBEDDED_DATABASE = "georocket"
    private const val DEFAULT_MAX_CONNECTION_IDLE_TIME_MS = 60000L
    private val sharedInstances = mutableMapOf<ConnectionString, SharedMongoClient>()

    private var mongodPort: Int = 0
    private var mongodExecutable: MongodExecutable? = null

    fun create(connectionString: ConnectionString): SharedMongoClient {
      return synchronized(this) {
        val result = sharedInstances.computeIfAbsent(connectionString) {
          val settings = MongoClientSettings.builder()
              .codecRegistry(CodecRegistries.fromCodecs(
                  StringCodec(), IntegerCodec(), BooleanCodec(),
                  DoubleCodec(), LongCodec(), BsonDocumentCodec(),
                  JsonObjectCodec(JsonObject())
              ))
              .applyToConnectionPoolSettings { builder ->
                builder.maxConnectionIdleTime(DEFAULT_MAX_CONNECTION_IDLE_TIME_MS, TimeUnit.MILLISECONDS)
              }
              .applyConnectionString(connectionString)
              .build()

          SharedMongoClient(connectionString, MongoClients.create(settings))
        }

        result.instanceCount++
        result
      }
    }

    suspend fun createEmbedded(vertx: Vertx, storagePath: String): SharedMongoClient {
      return vertx.executeBlocking<SharedMongoClient> { p ->
        synchronized(this) {
          if (mongodExecutable == null) {
            log.info("Launching embedded MongoDB instance ...")
            try {
              val runtimeConfig = Defaults.runtimeConfigFor(Command.MongoD, log).build()
              val starter = MongodStarter.getInstance(runtimeConfig)
              val port = Network.freeServerPort(Network.getLocalHost())
              val replication = Storage(storagePath, null, 0)
              val mongodConfig = MongodConfig.builder()
                .version(Version.Main.PRODUCTION)
                .net(Net(port, Network.localhostIsIPv6()))
                .replication(replication)
                .build()
              mongodExecutable = starter.prepare(mongodConfig)
              mongodExecutable!!.start()
              mongodPort = port
            } catch (t: Throwable) {
              mongodExecutable?.stop()
              mongodExecutable = null
              p.fail(t)
            }
          }
          val cs = ConnectionString("mongodb://localhost:$mongodPort/$DEFAULT_EMBEDDED_DATABASE")
          p.complete(create(cs))
        }
      }.await()
    }
  }

  override fun close() {
    synchronized(SharedMongoClient) {
      instanceCount--
      if (instanceCount == 0) {
        client.close()
        mongodExecutable?.stop()
        sharedInstances.remove(key)
      }
    }
  }
}
