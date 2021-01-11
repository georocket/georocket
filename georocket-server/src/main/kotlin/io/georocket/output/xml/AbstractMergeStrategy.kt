package io.georocket.output.xml

import io.georocket.storage.ChunkReadStream
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import rx.Completable

/**
 * Abstract base class for XML merge strategies
 * @author Michel Kraemer
 */
abstract class AbstractMergeStrategy : MergeStrategy {
  override var parents: List<XMLStartElement>? = null

  protected var isHeaderWritten = false
    private set

  /**
   * Merge the parent elements of a given [chunkMetadata] into the current
   * parent elements. Perform no checks.
   */
  protected abstract fun mergeParents(chunkMetadata: XMLChunkMeta): Completable

  override fun init(chunkMetadata: XMLChunkMeta): Completable {
    return canMerge(chunkMetadata)
        .flatMapCompletable { b ->
          if (b) {
            mergeParents(chunkMetadata)
          } else {
            Completable.error(IllegalArgumentException(
                "Chunk cannot be merged with this strategy"))
          }
        }
  }

  /**
   * Write the XML header and the parent elements
   * @param out the output stream to write to
   */
  private fun writeHeader(out: WriteStream<Buffer>) {
    out.write(Buffer.buffer(XMLHEADER))
    parents!!.forEach { e -> out.write(Buffer.buffer(e.toString())) }
  }

  override fun merge(chunk: ChunkReadStream, chunkMetadata: XMLChunkMeta,
      outputStream: WriteStream<Buffer>): Completable {
    return canMerge(chunkMetadata)
        .flatMapCompletable { b ->
          if (!b) {
            Completable.error(IllegalStateException(
                "Chunk cannot be merged with this strategy"))
          } else {
            if (!isHeaderWritten) {
              writeHeader(outputStream)
              isHeaderWritten = true
            }
            writeChunk(chunk, chunkMetadata, outputStream)
          }
        }
  }

  override fun finish(outputStream: WriteStream<Buffer>) {
    // close all parent elements
    for (e in parents!!.reversed()) {
      outputStream.write(Buffer.buffer("</" + e.name + ">"))
    }
  }

  companion object {
    /**
     * The default XML header written by the merger
     */
    const val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
  }
}
