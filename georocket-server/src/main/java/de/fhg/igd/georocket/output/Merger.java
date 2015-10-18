package de.fhg.igd.georocket.output;

import java.util.List;

import de.fhg.igd.georocket.storage.ChunkReadStream;
import de.fhg.igd.georocket.util.ChunkMeta;
import de.fhg.igd.georocket.util.XMLStartElement;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Merges XML chunks using various strategies to create a valid XML document
 * @author Michel Kraemer
 */
public class Merger {
  /**
   * Merge strategies
   */
  private static enum Strategy {
    /**
     * All chunks have the same parents with the same attributes and namespaces
     */
    ALL_SAME
  }
  
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  /**
   * True if the header has already been writte in {@link #merge(ChunkReadStream, ChunkMeta, WriteStream, Handler)}
   */
  private boolean headerWritten = false;
  
  /**
   * The merger strategy determined by {@link #init(ChunkMeta)}
   */
  private Strategy strategy = Strategy.ALL_SAME;
  
  /**
   * The XML parent elements of the first item that was passed to {@link #init(ChunkMeta)}
   */
  private List<XMLStartElement> firstParents;
  
  /**
   * Initializes this merger and determines the merge strategy. This method
   * must be called for all chunks that should be merged. After
   * {@link #merge(ChunkReadStream, ChunkMeta, WriteStream, Handler)}
   * has been called this method must not be called any more.
   * @param meta the chunk metadata
   */
  public void init(ChunkMeta meta) {
    if (firstParents == null) {
      firstParents = meta.getParents();
    } else {
      if (!firstParents.equals(meta.getParents())) {
        throw new UnsupportedOperationException("Cannot merge chunks. No valid strategy available.");
      }
    }
  }
  
  /**
   * Merge a chunk using the current merge strategy. The given chunk should
   * have been passed to {@link #init(ChunkMeta)} first. If it hasn't the method
   * may or may not accept it. If the chunk cannot be merged with the current
   * strategy, the method will throw an {@link IllegalArgumentException}
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @param handler will be called when the chunk has been merged
   * @throws IllegalArgumentException if the chunk cannot be merged
   * @throws IllegalStateException if {@link #init(ChunkMeta)} has never been called before
   */
  public void merge(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out,
      Handler<Void> handler) throws IllegalArgumentException {
    if (firstParents == null) {
      throw new IllegalStateException("You must call init() at least once");
    }
    switch (strategy) {
    case ALL_SAME:
      mergeSame(chunk, meta, out, handler);
      break;
    }
  }
  
  /**
   * Merge a chunk using the {@link Strategy#ALL_SAME} strategy
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @param handler will be called when the chunk has been merged
   * @throws IllegalArgumentException if the chunk cannot be merged
   */
  private void mergeSame(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out,
      Handler<Void> handler) throws IllegalArgumentException {
    if (!firstParents.equals(meta.getParents())) {
      throw new IllegalArgumentException("Chunk cannot be merged with this strategy");
    }
    
    if (!headerWritten) {
      // write the header and the parent elements
      out.write(Buffer.buffer(XMLHEADER));
      firstParents.forEach(e -> out.write(Buffer.buffer(e.toString())));
      headerWritten = true;
    }
    
    // write chunk to output stream
    int[] start = new int[] { meta.getStart() };
    int[] end = new int[] { meta.getEnd() };
    chunk.handler(buf -> {
      int s = Math.max(Math.min(start[0], buf.length()), 0);
      int e = Math.max(Math.min(end[0], buf.length()), 0);
      if (s != e) {
        out.write(buf.getBuffer(s, e));
      }
      start[0] -= buf.length();
      end[0] -= buf.length();
    });
    
    chunk.endHandler(handler);
  }
  
  /**
   * Finishes merging chunks and closes all open XML elements
   * @param out the stream to write the merged result to
   */
  public void finishMerge(WriteStream<Buffer> out) {
    // close all parent elements
    for (int i = firstParents.size() - 1; i >= 0; --i) {
      XMLStartElement e = firstParents.get(i);
      out.write(Buffer.buffer("</" + e.getName() + ">"));
    }
  }
}
