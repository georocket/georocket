package io.georocket.storage

/**
 * A cursor that can be used to iterate over items in a store
 * @author Michel Kraemer
 */
interface Cursor<T> {
  /**
   * Return `true` if there are more items to iterate over
   */
  suspend fun hasNext(): Boolean

  /**
   * Return the next item
   */
  suspend fun next(): T
}
