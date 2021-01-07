package io.georocket.index.mongodb

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.impl.codec.json.JsonObjectCodec
import org.bson.codecs.BooleanCodec
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DoubleCodec
import org.bson.codecs.IntegerCodec
import org.bson.codecs.LongCodec
import org.bson.codecs.StringCodec
import org.bson.codecs.configuration.CodecRegistries
import java.util.concurrent.TimeUnit

class SharedMongoClient(private val key: ConnectionString,
    private val client: MongoClient) : MongoClient by client {
  private var instanceCount = 0

  companion object {
    private const val DEFAULT_MAX_CONNECTION_IDLE_TIME_MS = 60000L
    private val sharedInstances = mutableMapOf<ConnectionString, SharedMongoClient>()

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
  }

  override fun close() {
    synchronized(SharedMongoClient) {
      instanceCount--
      if (instanceCount == 0) {
        client.close()
        sharedInstances.remove(key)
      }
    }
  }
}
