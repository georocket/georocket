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
  private val result = mutableMapOf<String, Any>()

  protected fun put(key: String, value: String) {
    // auto-convert to numbers
    val v = value.toLongOrNull() ?: value.toDoubleOrNull() ?: value

    // never overwrite attributes already collected!
    result.putIfAbsent(key, v)
  }

  override fun getResult() = mapOf("genAttrs" to result.entries.map { e ->
    mapOf("key" to e.key, "value" to e.value)
  })
}
