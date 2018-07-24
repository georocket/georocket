package io.georocket.output.xml;

import java.util.List;

import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.XMLStartElement;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import rx.Completable;

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
   * @return a Completable that will complete when the parents have been merged
   */
  protected abstract Completable mergeParents(XMLChunkMeta meta);
  
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
  public Completable init(XMLChunkMeta meta) {
    return canMerge(meta)
      .flatMapCompletable(b -> {
        if (b) {
          return mergeParents(meta);
        }
        return Completable.error(new IllegalArgumentException(
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
  public Completable merge(ChunkReadStream chunk, XMLChunkMeta meta,
      WriteStream<Buffer> out) {
    return canMerge(meta)
      .flatMapCompletable(b -> {
        if (!b) {
          return Completable.error(new IllegalStateException(
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
}
