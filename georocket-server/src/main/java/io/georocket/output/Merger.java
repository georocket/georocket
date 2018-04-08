package io.georocket.output;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Completable;

/**
 * Merges chunks to create a valid output document
 * @author Michel Kraemer
 * @param <T> the ChunkMeta type
 */
public interface Merger<T extends ChunkMeta> {
  /**
   * Initialize this merger and determine the merge strategy. This method
   * must be called for all chunks that should be merged. After
   * {@link #merge(ChunkReadStream, ChunkMeta, WriteStream)}
   * has been called this method must not be called any more.
   * @param meta the chunk metadata
   * @return a Completable that will complete when the merger has been
   * initialized with the given chunk
   */
  Completable init(T meta);
  
  /**
   * Merge a chunk using the current merge strategy. The given chunk should
   * have been passed to {@link #init(ChunkMeta)} first. If it hasn't
   * the method may or may not accept it. If the chunk cannot be merged with
   * the current strategy, the returned observable will fail.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @return a Completable that will complete when the chunk has been merged
   */
  Completable merge(ChunkReadStream chunk, T meta,
      WriteStream<Buffer> out);
  
  /**
   * Finishes merging chunks
   * @param out the stream to write the merged result to
   */
  void finish(WriteStream<Buffer> out);
  
  /**
   * Write a chunk unchanged to an output stream without doing further checks
   * @param chunk the chunk to write
   * @param meta the chunk's metadata
   * @param out the stream to write the chunk to
   * @return a Completable that will complete when the chunk has been written
   */
  default Completable writeChunk(ChunkReadStream chunk, ChunkMeta meta,
      WriteStream<Buffer> out) {
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
    
    ObservableFuture<Void> o = RxHelper.observableFuture();
    Handler<AsyncResult<Void>> handler = o.toHandler();
    
    chunk.exceptionHandler(err -> {
      chunk.endHandler(null);
      handler.handle(Future.failedFuture(err));
    });
    
    chunk.endHandler(v -> handler.handle(Future.succeededFuture()));
    
    return o.toCompletable();
  }
}
