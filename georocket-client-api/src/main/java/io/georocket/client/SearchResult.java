package io.georocket.client;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * Results returned by {@link StoreClient#startImport(ImportParams, io.vertx.core.Handler)}
 * when the data has been imported into GeoRocket
 * @since 1.3.0
 * @author Michel Kraemer
 */
public class SearchResult {
  private final ReadStream<Buffer> response;
  private final long unmergedChunks;

  /**
   * Create a new result
   * @param response a {@link ReadStream} from which the merged chunks
   * matching the search criteria can be read
   * @param unmergedChunks the number of chunks that could not be merged
   */
  public SearchResult(ReadStream<Buffer> response, long unmergedChunks) {
    this.response = response;
    this.unmergedChunks = unmergedChunks;
  }

  /**
   * Get a {@link ReadStream} from which the merged chunks matching the
   * search criteria can be read
   * @return the {@link ReadStream}
   */
  public ReadStream<Buffer> getResponse() {
    return response;
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
