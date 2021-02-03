package io.georocket.storage

/**
 * A cursor that can be used to iterate over items in a store
 * @author Michel Kraemer
 */
interface Cursor<T> {
  /**
   * Return `true` if there are more items to iterate over
   */
  suspend operator fun hasNext(): Boolean

  /**
   * Return the next item
   */
  suspend operator fun next(): T

  /**
   * Make this cursor available as an interator
   */
  operator fun iterator() = this
}
