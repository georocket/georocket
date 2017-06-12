package io.georocket.storage;

/**
 * Information about the cursor.
 * @author Andrej Sajenko
 */
public class CursorInfo {
  private String scrollId;
  private long totalHits;
  private int currentHits;

  /**
   * Standard constructor.
   * 
   * @param scrollId The scrollId to load the next frame (may be null).
   * @param totalHits Total number of elements.
   * @param currentHits The current number of elements loaded in a frame.
   */
  public CursorInfo(String scrollId, long totalHits, int currentHits) {
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
  public long getTotalHits() {
    return totalHits;
  }

  /**
   * @return The current number of elements loaded in a frame.
   */
  public int getCurrentHits() {
    return currentHits;
  }
}
