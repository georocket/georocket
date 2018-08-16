package io.georocket.client;

import io.vertx.core.streams.ReadStream;

/**
 * Results returned by {@link StoreClient#startImport(ImportParams, io.vertx.core.Handler)}
 * when the data has been imported into GeoRocket
 * @since 1.3.0
 * @author Michel Kraemer
 */
public class SearchResult {
  private final SearchReadStream response;

  /**
   * Create a new result
   * @param response a {@link ReadStream} from which the merged chunks
   * matching the search criteria can be read
   */
  public SearchResult(SearchReadStream response) {
    this.response = response;
  }

  /**
   * Get a {@link ReadStream} from which the merged chunks matching the
   * search criteria can be read
   * @return the {@link ReadStream}
   */
  public SearchReadStream getResponse() {
    return response;
  }
}
