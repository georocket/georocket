package io.georocket.output;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import rx.Observable;

public interface AsynchMerger extends Merger {

  /**
   * Initialize this merger and determine the merge strategy. This method
   * must be called for all chunks that should be merged. After
   * {@link #mergeObservable(ChunkReadStream, ChunkMeta, WriteStream)}
   * has been called this method must not be called any more.
   * @param meta the chunk metadata
   * @return an observable that completes once the merger has been
   * initialized with the given chunk
   */
  Observable<Void> initObservable(ChunkMeta meta);

  /**
   * Merge a chunk using the current merge strategy. The given chunk should
   * have been passed to {@link #initObservable(ChunkMeta)} first. If it hasn't
   * the method may or may not accept it. If the chunk cannot be merged with
   * the current strategy, the returned observable will fail.
   * @param chunk the chunk to merge
   * @param meta the chunk's metadata
   * @param out the stream to write the merged result to
   * @return an observable that completes once the chunk has been merged
   */
  Observable<Void> mergeObservable(ChunkReadStream chunk, ChunkMeta meta, WriteStream<Buffer> out);

}