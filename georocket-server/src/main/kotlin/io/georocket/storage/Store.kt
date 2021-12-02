package io.georocket.storage

import io.vertx.core.buffer.Buffer

/**
 * A store for chunks
 * @author Michel Kraemer
 */
interface Store {
  /**
   * Release all resources and close this store
   */
  fun close() {
    // nothing to do by default
  }

  /**
   * Add a [chunk] with given [chunkMetadata] and [indexMetadata] under the
   * given [layer] to the store. Return the path to the added chunk.
   */
  suspend fun add(chunk: Buffer, chunkMetadata: ChunkMeta,
      indexMetadata: IndexMeta, layer: String): String

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
}
