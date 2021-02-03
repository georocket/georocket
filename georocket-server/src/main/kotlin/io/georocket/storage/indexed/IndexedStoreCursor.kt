package io.georocket.storage.indexed

import io.georocket.storage.ChunkMeta
import io.georocket.storage.CursorInfo
import io.georocket.storage.StoreCursor
import io.vertx.core.Vertx

/**
 * Implementation of [StoreCursor] for indexed chunk stores
 * @author Michel Kraemer
 */
class IndexedStoreCursor(private val vertx: Vertx, private val search: String?,
    private val path: String) : StoreCursor {
  companion object {
    /**
     * The number of items to retrieve in one batch
     */
    private const val SIZE = 100
  }

  /**
   * This cursor uses [LegacyFrameCursor] to load the full datastore frame by frame.
   */
  private var currentFrameCursor: FrameCursor? = null

  /**
   * The current read position
   */
  private var pos = -1

  /**
   * The total number of items requested from the store
   */
  private val totalHits: Long get() = info.totalHits

  /**
   * The scrollId for elasticsearch
   */
  private val scrollId: String get() = info.scrollId

  override val chunkPath: String get() {
    val fc = currentFrameCursor ?: throw IllegalStateException(
        "Cursor has not been started yet")
    return fc.chunkPath
  }

  override val info: CursorInfo get() {
    val fc = currentFrameCursor ?: throw IllegalStateException(
        "Cursor has not been started yet")
    return fc.info
  }

  /**
   * Starts this cursor
   */
  suspend fun start(): IndexedStoreCursor {
    currentFrameCursor = FrameCursor(vertx, search, path, SIZE).start()
    return this
  }

  override suspend fun hasNext(): Boolean {
    return pos + 1 < totalHits
  }

  override suspend fun next(): ChunkMeta {
    val fc = currentFrameCursor ?: throw IllegalStateException(
        "Cursor has not been started yet")

    ++pos
    return when {
      pos >= totalHits ->
        throw IndexOutOfBoundsException("Cursor is beyond a valid position")

      fc.hasNext() -> fc.next()

      else -> {
        val newCurrentFrameCursor = FrameCursor(vertx, scrollId = scrollId).start()
        currentFrameCursor = newCurrentFrameCursor
        newCurrentFrameCursor.next()
      }
    }
  }
}
