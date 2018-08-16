package io.georocket.client;

/**
 * Results that become available after a {@link SearchReadStream} has been
 * read completely
 * @see SearchReadStream#endHandlerWithResult(io.vertx.core.Handler)
 * @since 1.3.0
 * @author Michel Kraemer
 */
public class SearchReadStreamResult {
  private final long unmergedChunks;

  /**
   * Create a new result
   * @param unmergedChunks the number of chunks that could not be merged
   */
  public SearchReadStreamResult(long unmergedChunks) {
    this.unmergedChunks = unmergedChunks;
  }

  /**
   * <p>Get the number of chunks that could not be merged. Unmerged chunks can
   * have the following causes:</p>
   * <ul>
   * <li>Chunks were added to GeoRocket's store while merging was in progress</li>
   * <li>Optimistic merging was enabled and some chunks did not fit to the
   * search result</li>
   * </ul>
   * @see SearchParams#setOptimisticMerging(boolean)
   * @return the number of unmerged chunks
   */
  public long getUnmergedChunks() {
    return unmergedChunks;
  }
}
