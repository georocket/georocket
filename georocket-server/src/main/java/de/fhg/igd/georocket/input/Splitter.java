package de.fhg.igd.georocket.input;

import de.fhg.igd.georocket.storage.ChunkMeta;
import de.fhg.igd.georocket.util.XMLStreamEvent;

/**
 * Splits XML tokens and returns chunks
 * @author Michel Kraemer
 */
public interface Splitter {
  /**
   * Result of the {@link Splitter#onEvent(XMLStreamEvent)} method. Holds
   * a chunk and its metadata.
   */
  public static class Result {
    private final String chunk;
    private final ChunkMeta meta;
    
    /**
     * Create a new result object
     * @param chunk the chunk
     * @param meta the chunk's metadata
     */
    public Result(String chunk, ChunkMeta meta) {
      this.chunk = chunk;
      this.meta = meta;
    }
    
    /**
     * @return the chunk
     */
    public String getChunk() {
      return chunk;
    }
    
    /**
     * @return the chunk's metadata
     */
    public ChunkMeta getMeta() {
      return meta;
    }
  }
  
  /**
   * Will be called on every XML event
   * @param event the XML event
   * @return a new {@link Result} object (containing chunk and metadata) or
   * <code>null</code> if no result was produced
   */
  Result onEvent(XMLStreamEvent event);
}
