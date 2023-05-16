package io.georocket.output.xml

import io.georocket.output.Merger
import io.georocket.storage.XmlChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

/**
 * Merges XML chunks using various strategies to create a valid XML document
 * @param optimistic `true` if chunks should be merged optimistically
 * without prior initialization
 * @author Michel Kraemer
 */
class XMLMerger(private val optimistic: Boolean) : Merger<XmlChunkMeta> {
  /**
   * The merger strategy determined by [init]
   */
  private var strategy: MergeStrategy = AllSameStrategy()

  /**
   * `true` if [init] has been called at least once
   */
  private var initialized = false

  /**
   * `true` if [merge] has been called at least once
   */
  private var mergeStarted = false

  /**
   * Returns the next merge strategy (depending on the current one)
   */
  private fun nextStrategy(): MergeStrategy {
    if (strategy is AllSameStrategy) {
      return MergeNamespacesStrategy()
    }
    throw UnsupportedOperationException(
      "Cannot merge chunks. No valid " +
          "strategy available."
    )
  }

  override fun init(chunkMetadata: XmlChunkMeta) {
    if (mergeStarted) {
      throw IllegalStateException(
        "You cannot initialize the merger anymore " +
            "after merging has begun"
      )
    }

    while (!strategy.canMerge(chunkMetadata)) {
      // current strategy cannot merge the chunk
      val ns = nextStrategy()
      ns.parents = strategy.parents
      strategy = ns
    }

    // current strategy is able to handle the chunk
    initialized = true
    strategy.init(chunkMetadata)
  }

  override suspend fun merge(
    chunk: Buffer,
    chunkMetadata: XmlChunkMeta,
    outputStream: WriteStream<Buffer>
  ) {
    mergeStarted = true
    if (!initialized) {
      if (optimistic) {
        strategy = AllSameStrategy()
        strategy.init(chunkMetadata)
        initialized = true
      } else {
        throw IllegalStateException("You must call init() at least once")
      }
    }
    strategy.merge(chunk, chunkMetadata, outputStream)
  }

  override fun finish(outputStream: WriteStream<Buffer>) {
    strategy.finish(outputStream)
  }
}
