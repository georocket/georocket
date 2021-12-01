package io.georocket.index

import io.georocket.constants.AddressConstants
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.query.DefaultQueryCompiler
import io.georocket.util.FilteredServiceLoader
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch

/**
 * Generic methods for handling chunk metadata
 * @author Michel Kraemer
 */
class MetadataVerticle : CoroutineVerticle() {
  /**
   * The GeoRocket index
   */
  private lateinit var index: Index

  /**
   * A list of [MetaIndexerFactory] objects
   */
  private lateinit var metaIndexerFactories: List<MetaIndexerFactory>

  /**
   * A list of [IndexerFactory] objects
   */
  private lateinit var indexerFactories: List<IndexerFactory>

  override suspend fun start() {
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    metaIndexerFactories = FilteredServiceLoader.load(MetaIndexerFactory::class.java).toList()
    indexerFactories = FilteredServiceLoader.load(IndexerFactory::class.java).toList()

    index = MongoDBIndex(vertx)

    vertx.eventBus().consumer(AddressConstants.METADATA_GET_ATTRIBUTE_VALUES,
      this::onGetAttributeValues)
    vertx.eventBus().consumer(AddressConstants.METADATA_GET_PROPERTY_VALUES,
      this::onGetPropertyValues)
    vertx.eventBus().consumer(AddressConstants.METADATA_SET_PROPERTIES,
      this::onSetProperties)
    vertx.eventBus().consumer(AddressConstants.METADATA_REMOVE_PROPERTIES,
      this::onRemoveProperties)
    vertx.eventBus().consumer(AddressConstants.METADATA_APPEND_TAGS,
      this::onAppendTags)
    vertx.eventBus().consumer(AddressConstants.METADATA_REMOVE_TAGS,
      this::onRemoveTags)
  }

  private fun onGetAttributeValues(msg: Message<JsonObject>) {
    launch {
      val body = msg.body()
      val search = body.getString("search") ?: ""
      val path = body.getString("path")
      val attributeName = body.getString("attribute")

      val query = DefaultQueryCompiler(metaIndexerFactories + indexerFactories).compileQuery(search, path)

      val values = index.getAttributeValues(query, attributeName)

      msg.reply(JsonArray(values))
    }
  }

  private fun onGetPropertyValues(msg: Message<JsonObject>) {
    launch {
      val body = msg.body()
      val search = body.getString("search") ?: ""
      val path = body.getString("path")
      val propertyName = body.getString("property")

      val query = DefaultQueryCompiler(metaIndexerFactories + indexerFactories).compileQuery(search, path)

      val values = index.getPropertyValues(query, propertyName)

      msg.reply(JsonArray(values))
    }
  }

  /**
   * Set properties of a list of chunks
   */
  private fun onSetProperties(msg: Message<JsonObject>) {
    launch {
      val body = msg.body()
      val properties = body.getJsonObject("properties")?.map
      val search = body.getString("search") ?: ""
      val path = body.getString("path")

      val query = DefaultQueryCompiler(metaIndexerFactories + indexerFactories).compileQuery(search, path)

      if (properties != null) {
        index.setProperties(query, properties)
      }

      msg.reply(null)
    }
  }

  /**
   * Remove properties of a list of chunks
   */
  private fun onRemoveProperties(msg: Message<JsonObject>) {
    launch {
      val body = msg.body()
      val properties = body.getJsonArray("properties")?.filterIsInstance<String>() ?: emptyList()
      val search = body.getString("search") ?: ""
      val path = body.getString("path")

      val query = DefaultQueryCompiler(metaIndexerFactories + indexerFactories).compileQuery(search, path)

      index.removeProperties(query, properties)

      msg.reply(null)
    }
  }

  /**
   * Append tags to a list of chunks
   */
  private fun onAppendTags(msg: Message<JsonObject>) {
    launch {
      val body = msg.body()
      val tags = body.getJsonArray("tags")?.filterIsInstance<String>() ?: emptyList()
      val search = body.getString("search") ?: ""
      val path = body.getString("path")

      val query = DefaultQueryCompiler(metaIndexerFactories + indexerFactories).compileQuery(search, path)

      index.addTags(query, tags)

      msg.reply(null)
    }
  }

  /**
   * Remove tags of a list of chunks
   */
  private fun onRemoveTags(msg: Message<JsonObject>) {
    launch {
      val body = msg.body()
      val tags = body.getJsonArray("tags")?.filterIsInstance<String>() ?: emptyList()
      val search = body.getString("search") ?: ""
      val path = body.getString("path")

      val query = DefaultQueryCompiler(metaIndexerFactories + indexerFactories).compileQuery(search, path)

      index.removeTags(query, tags)

      msg.reply(null)
    }
  }
}
