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

/**
 * Abstract base class for XML merge strategies
 * @author Michel Kraemer
 */
public abstract class AbstractMergeStrategy implements MergeStrategy {
  /**
   * The default XML header written by the merger
   */
  public static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  protected AsyncResult<Boolean> ASYNC_TRUE = Future.succeededFuture(true);
  protected AsyncResult<Boolean> ASYNC_FALSE = Future.succeededFuture(false);
  
  /**
   * The XML parent elements
   */
  private List<XMLStartElement> parents;
  
  /**
   * True if the header has already been written in
   * {@link #merge(ChunkReadStream, XMLChunkMeta, WriteStream, Handler)}
   */
  private boolean headerWritten = false;
  
  /**
   * Merge the parent elements of a given chunk into the current parent
   * elements. Perform no checks.
   * @param meta the chunk metadata containing the parents to merge
   * @param handler will be called when the parents have been merged
   */
  protected abstract void mergeParents(XMLChunkMeta meta,
      Handler<AsyncResult<Void>> handler);
  
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
  public void init(XMLChunkMeta meta, Handler<AsyncResult<Void>> handler) {
    canMerge(meta, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        mergeParents(meta, handler);
      }
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
  public void merge(ChunkReadStream chunk, XMLChunkMeta meta, WriteStream<Buffer> out,
      Handler<AsyncResult<Void>> handler) {
    canMerge(meta, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      if (!ar.result()) {
        handler.handle(Future.failedFuture(new IllegalArgumentException(
            "Chunk cannot be merged with this strategy")));
        return;
      }
      if (!headerWritten) {
        writeHeader(out);
        headerWritten = true;
      }
      writeChunk(chunk, meta, out, handler);
    });
  }
  
  @Override
  public void finishMerge(WriteStream<Buffer> out) {
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
   * @param handler will be called when the chunk has been written
   */
  protected void writeChunk(ChunkReadStream chunk, ChunkMeta meta,
      WriteStream<Buffer> out, Handler<AsyncResult<Void>> handler) {
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
}
