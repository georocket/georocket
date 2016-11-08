package io.georocket.output.xml;

import java.util.List;

import io.georocket.storage.XMLChunkMeta;
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
 * Abstract base class for XML merge strategies
 * @author Michel Kraemer
 */
public abstract class AbstractMergeStrategy implements MergeStrategy {
  /**
   * The default XML header written by the merger
   */
  public static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  /**
   * The XML parent elements
   */
  private List<XMLStartElement> parents;
  
  /**
   * True if the header has already been written in
   * {@link #merge(ChunkReadStream, XMLChunkMeta, WriteStream)}
   */
  private boolean headerWritten = false;
  
  /**
   * Merge the parent elements of a given chunk into the current parent
   * elements. Perform no checks.
   * @param meta the chunk metadata containing the parents to merge
   * @return an observable that will emit one item when the parents have
   * been merged
   */
  protected abstract Observable<Void> mergeParents(XMLChunkMeta meta);
  
  /**
   * @return true if the header has already been write to the output stream
   */
  protected boolean isHeaderWritten() {
    return headerWritten;
  }
  
  @Override
  public void setParents(List<XMLStartElement> parents) {
    this.parents = parents;
  }
  
  @Override
  public List<XMLStartElement> getParents() {
    return parents;
  }
  
  @Override
  public Observable<Void> init(XMLChunkMeta meta) {
    return canMerge(meta)
      .flatMap(b -> {
        if (b) {
          return mergeParents(meta);
        }
        return Observable.error(new IllegalArgumentException(
            "Chunk cannot be merged with this strategy"));
      });
  }
  
  /**
   * Write the XML header and the parent elements
   * @param out the output stream to write to
   */
  private void writeHeader(WriteStream<Buffer> out) {
    out.write(Buffer.buffer(XMLHEADER));
    parents.forEach(e -> out.write(Buffer.buffer(e.toString())));
  }
  
  @Override
  public Observable<Void> merge(ChunkReadStream chunk, XMLChunkMeta meta,
      WriteStream<Buffer> out) {
    return canMerge(meta)
      .flatMap(b -> {
        if (!b) {
          return Observable.error(new IllegalArgumentException(
              "Chunk cannot be merged with this strategy"));
        }
        if (!headerWritten) {
          writeHeader(out);
          headerWritten = true;
        }
        return writeChunk(chunk, meta, out);
      });
  }
  
  @Override
  public void finish(WriteStream<Buffer> out) {
    // close all parent elements
    for (int i = parents.size() - 1; i >= 0; --i) {
      XMLStartElement e = parents.get(i);
      out.write(Buffer.buffer("</" + e.getName() + ">"));
    }
  }
  
  /**
   * Write a chunk unchanged to an output stream without doing further checks
   * @param chunk the chunk to write
   * @param meta the chunk's metadata
   * @param out the stream to write the chunk to
   * @return an observable that will emit one item when the chunk has been written
   */
  protected Observable<Void> writeChunk(ChunkReadStream chunk, ChunkMeta meta,
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
    
    return o;
  }
}
