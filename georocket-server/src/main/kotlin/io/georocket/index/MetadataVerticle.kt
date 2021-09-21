package io.georocket.index

import io.georocket.constants.AddressConstants
import io.georocket.util.FilteredServiceLoader
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.coroutines.CoroutineVerticle
import java.util.function.BiFunction
import java.util.stream.Stream

/**
 * Generic methods for handling chunk metadata
 * @author Michel Kraemer
 */
class MetadataVerticle : CoroutineVerticle() {
  companion object {
    private val log = LoggerFactory.getLogger(IndexerVerticle::class.java)
  }

  /**
   * A list of [IndexerFactory] objects
   */
  private lateinit var indexerFactories: List<IndexerFactory>

  /**
   * A function that extracts values of indexed attributes from Elasticsearch
   * source objects
   */
  private lateinit var indexedAttributeExtractor: BiFunction<JsonObject, String, Stream<Any>?>

  override suspend fun start() {
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    indexerFactories = FilteredServiceLoader.load(IndexerFactory::class.java).toList()

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
    TODO()
  }

  private fun onGetPropertyValues(msg: Message<JsonObject>) {
    TODO()
  }

  /**
   * Set properties of a list of chunks
   */
  private fun onSetProperties(msg: Message<JsonObject>) {
    TODO()
  }

  /**
   * Remove properties of a list of chunks
   */
  private fun onRemoveProperties(msg: Message<JsonObject>) {
    TODO()
  }

  /**
   * Append tags to a list of chunks
   */
  private fun onAppendTags(msg: Message<JsonObject>) {
    TODO()
  }

  /**
   * Remove tags of a list of chunks
   */
  private fun onRemoveTags(msg: Message<JsonObject>) {
    TODO()
  }
}
