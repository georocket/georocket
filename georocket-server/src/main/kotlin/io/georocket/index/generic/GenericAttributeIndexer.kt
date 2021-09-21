package io.georocket.index.generic

import io.georocket.index.Indexer

/**
 * Base class for all indexers that find generic attributes (i.e. properties,
 * key-value pairs) in chunks
 * @author Michel Kraemer
 */
open class GenericAttributeIndexer : Indexer {
  /**
   * Map collecting all attributes parsed
   */
  private val result = mutableMapOf<String, String>()

  protected fun put(key: String, value: String) {
    // never overwrite attributes already collected!
    result.putIfAbsent(key, value)
  }

  override fun getResult() = mapOf("genAttrs" to result)
}
