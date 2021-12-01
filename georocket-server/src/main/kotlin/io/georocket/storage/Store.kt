package io.georocket.storage

import io.vertx.core.buffer.Buffer

/**
 * A store for chunks
 * @author Michel Kraemer
 */
interface Store {
  /**
   * Add a [chunk] with given [chunkMetadata] and [indexMetadata] under the
   * given [layer] to the store
   */
  suspend fun add(chunk: Buffer, chunkMetadata: ChunkMeta,
      indexMetadata: IndexMeta, layer: String)

  /**
   * Get a chunk with a given [path] from the store
   */
  suspend fun getOne(path: String): Buffer

  /**
   * Delete all chunks from the store that match a given [search] query and
   * [path]. Callers should pass a [deleteMetadata] object with a unique
   * `correlationId` so the deletion process can be tracked correctly.
   */
  suspend fun delete(search: String?, path: String, deleteMetadata: DeleteMeta)

  /**
   * Get a number of chunks from the store using a given [search] query and [path]
   */
  suspend fun get(search: String?, path: String): StoreCursor

  /**
   * Start scrolling over chunks matching the given [search] query and [path]
   * but load only one frame with a given [size].
   */
  suspend fun scroll(search: String?, path: String, size: Int): StoreCursor

  /**
   * Continue scrolling with a given [scrollId]
   */
  suspend fun scroll(scrollId: String): StoreCursor

  /**
   * Get all values of the [attribute] of all chunks matching a given [search] query [path]
   */
  suspend fun getAttributeValues(search: String?, path: String, attribute: String): List<Any?>

  /**
   * Get all values of the [property] of all chunks matching a given [search] query [path]
   */
  suspend fun getPropertyValues(search: String?, path: String, property: String): List<Any?>

  /**
   * Set the [properties] of a list of chunks selected by [search] and [path]
   */
  suspend fun setProperties(search: String?, path: String, properties: Map<String, Any>)

  /**
   * Remove the [properties] of a list of chunks selected by [search] and [path]
   */
  suspend fun removeProperties(search: String?, path: String, properties: List<String>)

  /**
   * Append [tags] to a list of chunks selected by [search] and [path]
   */
  suspend fun appendTags(search: String?, path: String, tags: List<String>)

  /**
   * Remove [tags] from a list of chunks selected by [search] and [path]
   */
  suspend fun removeTags(search: String?, path: String, tags: List<String>)
}
