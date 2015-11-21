package io.georocket.output;

import java.util.List;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.util.XMLStartElement;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

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
   * strategy, the method will call the given handler with a failed result.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @param handler will be called when the chunk has been merged
   */
  public void merge(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out,
      Handler<AsyncResult<Void>> handler) {
    if (firstParents == null) {
      handler.handle(Future.failedFuture(new IllegalStateException(
          "You must call init() at least once")));
      return;
    }
    switch (strategy) {
    case ALL_SAME:
      mergeSame(chunk, meta, out, handler);
      break;
    }
  }
  
  /**
   * Merge a chunk using the current merge strategy. The given chunk should
   * have been passed to {@link #init(ChunkMeta)} first. If it hasn't the method
   * may or may not accept it. If the chunk cannot be merged with the current
   * strategy, the returned observable will fail.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @return an observable that completes once the chunk has been merged
   */
  public Observable<Void> mergeObservable(ChunkReadStream chunk, ChunkMeta meta,
      WriteStream<Buffer> out) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    merge(chunk, meta, out, o.toHandler());
    return o;
  }
  
  /**
   * Merge a chunk using the {@link Strategy#ALL_SAME} strategy
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @param handler will be called when the chunk has been merged
   */
  private void mergeSame(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out,
      Handler<AsyncResult<Void>> handler) {
    if (!firstParents.equals(meta.getParents())) {
      handler.handle(Future.failedFuture(new IllegalArgumentException(
          "Chunk cannot be merged with this strategy")));
      return;
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
    
    chunk.exceptionHandler(err -> {
      chunk.endHandler(null);
      handler.handle(Future.failedFuture(err));
    });
    
    chunk.endHandler(v -> handler.handle(Future.succeededFuture()));
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
