package io.georocket.storage;

/**
 * Information about one frame.
 * 
 * @author Andrej Sajenko
 */
public class FrameInfo {
  
  private String scrollId;
  private Long totalHits;
  private Integer currentHits;

  /**
   * Standard constructor.
   * 
   * @param scrollId The scrollId to load the next frame (may be null).
   * @param totalHits Total number of elements.
   * @param currentHits The current number of elements loaded in a frame.
   */
  public FrameInfo(String scrollId, Long totalHits, Integer currentHits) {
    this.scrollId = scrollId;
    this.totalHits = totalHits;
    this.currentHits = currentHits;
  }

  /**
   * @return The strollId to load the next frame (can be null).
   */
  public String getScrollId() {
    return scrollId;
  }

  /**
   * @return The number of all elements.
   */
  public Long getTotalHits() {
    return totalHits;
  }

  /**
   * @return The current number of elements loaded in a frame.
   */
  public Integer getCurrentHits() {
    return currentHits;
  }
}
