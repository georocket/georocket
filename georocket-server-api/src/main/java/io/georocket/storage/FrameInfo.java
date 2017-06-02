package io.georocket.storage;

/**
 * @author Andrej Sajenko
 */
public class FrameInfo {
  
  private String scrollId;
  private Long totalHits;
  private Integer currentHits;

  public FrameInfo(String scrollId, Long totalHits, Integer currentHits) {
    this.scrollId = scrollId;
    this.totalHits = totalHits;
    this.currentHits = currentHits;
  }

  public String getScrollId() {
    return scrollId;
  }

  public Long getTotalHits() {
    return totalHits;
  }

  public Integer getCurrentHits() {
    return currentHits;
  }
}
