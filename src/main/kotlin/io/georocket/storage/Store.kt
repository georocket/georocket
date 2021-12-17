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

  fun makePath(indexMetadata: IndexMeta, layer: String): String

  /**
   * Add a [chunk] with given [path] to the store
   */
  suspend fun add(chunk: Buffer, path: String)

  /**
   * Bulk-add many [chunks] with paths to the store
   */
  suspend fun addMany(chunks: Collection<Pair<Buffer, String>>) {
    chunks.forEach { add(it.first, it.second) }
  }

  /**
   * Get a chunk with a given [path] from the store
   */
  suspend fun getOne(path: String): Buffer

  /**
   * Delete all chunks from the store that match a given [paths]
   */
  suspend fun delete(paths: Collection<String>)
}
