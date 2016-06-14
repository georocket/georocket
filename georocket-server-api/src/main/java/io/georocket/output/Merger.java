package io.georocket.output;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

public interface Merger {

  /**
   * Initialize this merger and determine the merge strategy. This method
   * must be called for all chunks that should be merged. After
   * {@link #merge(ChunkReadStream, ChunkMeta, WriteStream, Handler)}
   * has been called this method must not be called any more.
   * @param meta the chunk metadata
   * @param handler will be called when the merger has been initialized with
   * the given chunk
   */
  void init(ChunkMeta meta, Handler<AsyncResult<Void>> handler);

  /**
   * Merge a chunk using the current merge strategy. The given chunk should
   * have been passed to {@link #init(ChunkMeta, Handler)} first. If it hasn't
   * the method may or may not accept it. If the chunk cannot be merged with
   * the current strategy, the method will call the given handler with a
   * failed result.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @param handler will be called when the chunk has been merged
   */
  void merge(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out, Handler<AsyncResult<Void>> handler);

  /**
   * Finishes merging chunks and closes all open XML elements
   * @param out the stream to write the merged result to
   */
  void finishMerge(WriteStream<Buffer> out);

}