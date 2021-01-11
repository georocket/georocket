package io.georocket.output.xml

import io.georocket.output.Merger
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.XMLChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import rx.Completable

/**
 * Merges XML chunks using various strategies to create a valid XML document
 * @param optimistic `true` if chunks should be merged optimistically
 * without prior initialization
 * @author Michel Kraemer
 */
class XMLMerger(private val optimistic: Boolean) : Merger<XMLChunkMeta> {
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
    throw UnsupportedOperationException("Cannot merge chunks. No valid " +
        "strategy available.")
  }

  override fun init(chunkMetadata: XMLChunkMeta): Completable {
    if (mergeStarted) {
      return Completable.error(IllegalStateException("You cannot "
          + "initialize the merger anymore after merging has begun"))
    }

    return strategy.canMerge(chunkMetadata)
        .flatMapCompletable { canMerge ->
          if (canMerge) {
            initialized = true

            // current strategy is able to handle the chunk
            return@flatMapCompletable strategy.init(chunkMetadata)
          }

          // current strategy cannot merge the chunk. select next one and retry.
          val ns = nextStrategy()
          ns.parents = strategy.parents
          strategy = ns

          // TODO remove recursiveness
          init(chunkMetadata)
        }
  }

  override fun merge(chunk: ChunkReadStream, chunkMetadata: XMLChunkMeta,
      outputStream: WriteStream<Buffer>): Completable {
    mergeStarted = true
    var c = Completable.complete()
    if (!initialized) {
      if (optimistic) {
        strategy = AllSameStrategy()
        c = strategy.init(chunkMetadata)
      } else {
        return Completable.error(IllegalStateException(
            "You must call init() at least once"))
      }
    }
    return c.andThen(strategy.merge(chunk, chunkMetadata, outputStream))
  }

  override fun finish(outputStream: WriteStream<Buffer>) {
    strategy.finish(outputStream)
  }
}
