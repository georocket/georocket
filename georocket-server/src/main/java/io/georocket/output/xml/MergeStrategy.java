package io.georocket.output.xml;

import java.util.List;

import io.georocket.storage.XMLChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.util.XMLStartElement;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * A merge strategy for XML chunks
 * @author Michel Kraemer
 */
public interface MergeStrategy {
  /**
   * Set XML parent elements for the chunks to merge
   * @param parents the parent elements
   */
  void setParents(List<XMLStartElement> parents);
  
  /**
   * @return the merged XML parent elements
   */
  List<XMLStartElement> getParents();
  
  /**
   * Check if a chunk with the given metadata can be merged and call a
   * handler with the result
   * @param meta the chunk metadata
   * @param handler will be called with the result of the operation
   */
  void canMerge(XMLChunkMeta meta, Handler<AsyncResult<Boolean>> handler);
  
  /**
   * Initialize this merge strategy. This method must be called for all chunks that
   * should be merged. After {@link #merge(ChunkReadStream, XMLChunkMeta, WriteStream, Handler)}
   * has been called this method must not be called any more.
   * @param meta the chunk metadata
   * @param handler will be called when the merger has been initialized with
   * the given chunk
   */
  void init(XMLChunkMeta meta, Handler<AsyncResult<Void>> handler);
  
  /**
   * Merge an XML chunk. The given chunk should* have been passed to
   * {@link #init(XMLChunkMeta, Handler)} first. If it hasn't the method may or
   * may not accept it.  If the chunk cannot be merged the method will call
   * the given handler with a failed result.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @param handler will be called when the chunk has been merged
   */
  public void merge(ChunkReadStream chunk, XMLChunkMeta meta, WriteStream<Buffer> out,
      Handler<AsyncResult<Void>> handler);
  
  /**
   * Finishes merging chunks and closes all open XML elements
   * @param out the stream to write the merged result to
   */
  void finishMerge(WriteStream<Buffer> out);
}
