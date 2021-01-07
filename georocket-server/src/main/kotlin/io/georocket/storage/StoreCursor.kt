package io.georocket.storage

/**
 * A cursor that can be used to iterate over chunks in a [Store]
 * @author Michel Kraemer
 */
interface StoreCursor : Cursor<ChunkMeta> {
  /**
   * Return the absolute path to the chunk that has been produced by the last
   * call to [next]
   */
  val chunkPath: String

  /**
   * Return info about the set of items
   */
  val info: CursorInfo
}
