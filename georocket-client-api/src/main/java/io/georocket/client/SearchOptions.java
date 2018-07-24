package io.georocket.client;

import java.util.Objects;

/**
 * Parameters that can be passed to {@link StoreClient#search(SearchOptions, io.vertx.core.Handler)}
 * @since 1.3.0
 * @author Michel Kraemer
 */
public class SearchOptions {
  private String query;
  private String layer;

  /**
   * Set a search query specifying which chunks to return
   * @param query the query (may be {@link null} if all chunks should
   * be returned)
   * @return a reference to this, so the API can be used fluently
   */
  public SearchOptions setQuery(String query) {
    this.query = query;
    return this;
  }

  /**
   * Get the search query specifying which chunks will be returned
   * @return the query (may be {@link null} if all chunks will
   * be returned)
   */
  public String getQuery() {
    return query;
  }

  /**
   * Set the name of the layer where to search for chunks recursively
   * @param layer the layer (may be {@code null} if chunks should be
   * searched in the root layer recursively)
   * @return a reference to this, so the API can be used fluently
   */
  public SearchOptions setLayer(String layer) {
    this.layer = layer;
    return this;
  }

  /**
   * Get the name of the layer where to search for chunks recursively
   * @return the layer (may be {@code null} if chunks will be
   * searched in the root layer recursively)
   */
  public String getLayer() {
    return layer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchOptions that = (SearchOptions)o;
    return Objects.equals(query, that.query) &&
      Objects.equals(layer, that.layer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, layer);
  }
}
