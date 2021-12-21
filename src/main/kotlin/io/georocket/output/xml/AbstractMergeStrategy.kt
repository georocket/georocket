package io.georocket.output.xml

import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

/**
 * Abstract base class for XML merge strategies
 * @author Michel Kraemer
 */
abstract class AbstractMergeStrategy : MergeStrategy {
  companion object {
    /**
     * The default XML header written by the merger
     */
    const val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
  }

  override var parents: List<XMLStartElement>? = null

  protected var isHeaderWritten = false
    private set

  /**
   * Merge the parent elements of a given [chunkMetadata] into the current
   * parent elements. Perform no checks.
   */
  protected abstract fun mergeParents(chunkMetadata: XMLChunkMeta)

  override fun init(chunkMetadata: XMLChunkMeta) {
    if (!canMerge(chunkMetadata)) {
      throw IllegalArgumentException("Chunk cannot be merged with this strategy")
    }

    mergeParents(chunkMetadata)
  }

  /**
   * Write the XML header and the parent elements
   * @param out the output stream to write to
   */
  private fun writeHeader(out: WriteStream<Buffer>) {
    out.write(Buffer.buffer(XMLHEADER))
    parents!!.forEach { e -> out.write(Buffer.buffer(e.toString())) }
  }

  override suspend fun merge(chunk: Buffer, chunkMetadata: XMLChunkMeta,
      outputStream: WriteStream<Buffer>) {
    if (!canMerge(chunkMetadata)) {
      throw IllegalStateException("Chunk cannot be merged with this strategy")
    }

    if (!isHeaderWritten) {
      writeHeader(outputStream)
      isHeaderWritten = true
    }
    outputStream.write(chunk)
  }

  override fun finish(outputStream: WriteStream<Buffer>) {
    // close all parent elements
    for (e in parents!!.reversed()) {
      outputStream.write(Buffer.buffer("</" + e.name + ">"))
    }
  }
}
