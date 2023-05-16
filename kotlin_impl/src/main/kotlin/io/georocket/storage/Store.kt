package io.georocket.storage

import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.georocket.util.chunked
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

/**
 * A store for chunks
 * @author Michel Kraemer
 */
interface Store {
  /**
   * Release all resources and close this store
   */
  suspend fun close() {
    // nothing to do by default
  }

  fun makePath(indexMetadata: IndexMeta): String {
    val path = indexMetadata.layer.ifEmpty { "/" }
    return PathUtils.join(path, indexMetadata.correlationId + UniqueID.next())
  }

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
   * Get chunks with the given [paths].
   * Implementors might use bulk operations to make [getMany] more efficient than multiple calls to [getOne].
   */
  suspend fun getMany(paths: List<String>): Map<String, Buffer> {
    // default implementation: Call getOne for each requested chunk
    return paths.associateWith { path ->
      getOne(path)
    }
  }

  /**
   * Get chunks with the given [paths].
   * This will fetch chunks in batches of size [maxChunksPerBatch] (see [getMany]). Up to [maxConcurrentBatches]
   * batches will be processed in parallel.
   */
  suspend fun<T> getManyParallelBatched(
    paths: Flow<Pair<String, T>>,
    maxChunksPerBatch: Int = 100,
    maxConcurrentBatches: Int = 20
  ): Flow<Pair<Buffer, T>> {

    // Scope is valid until the stream has been fully collected.
    val scope = CoroutineScope(Dispatchers.Default)

    return paths
      .chunked(maxChunksPerBatch)
      .map { batch ->
        scope.async {
          val results = getMany(batch.map { it.first })
          batch.map { (filename, t) ->
            val buf = results[filename] ?: throw NoSuchElementException("Could not find chunk with path `$filename'")
            buf to t
          }
        }
      }
      .buffer(capacity = maxConcurrentBatches)
      .transform { it.await().forEach { result -> emit(result) } }
      .onCompletion { scope.cancel() }
  }

  /**
   * Delete all chunks from the store that match a given [paths]. Return
   * the number of chunks deleted.
   */
  suspend fun delete(paths: Collection<String>): Long
}
