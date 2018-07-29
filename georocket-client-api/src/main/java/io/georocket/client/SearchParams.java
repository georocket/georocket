package io.georocket.client;

import java.util.Objects;

/**
 * Parameters that can be passed to {@link StoreClient#search(SearchParams, io.vertx.core.Handler)}
 * @since 1.3.0
 * @author Michel Kraemer
 */
public class SearchParams {
  private String query;
  private String layer;
  private boolean optimisticMerging;

  /**
   * Set a search query specifying which chunks to return
   * @param query the query (may be {@code null} if all chunks should
   * be returned)
   * @return a reference to this, so the API can be used fluently
   */
  public SearchParams setQuery(String query) {
    this.query = query;
    return this;
  }

  /**
   * Get the search query specifying which chunks will be returned
   * @return the query (may be {@code null} if all chunks will
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
  public SearchParams setLayer(String layer) {
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

  /**
   * <p>Enable or disable optimistic merging.</p>
   * <p>Optimistic merging can tremendously reduce search latency by
   * immediately returning chunks as soon as they have been fetched
   * from GeoRocket's store without initialization. This is particularly
   * useful if the data is homogeneous. The drawback is that GeoRocket might
   * not be able to merge chunks that do not fit to the overall search result.
   * For example, if the store contains XML files and all chunks are
   * homogeneous and have the same XML namespaces they can be merged perfectly
   * with optimistic merging enabled. However, if some chunks have different
   * namespaces, GeoRocket will not be able to merge them if the XML header
   * of the search result has already been sent to the client.</p>
   * <p>Chunks that cannot be merged will be ignored by GeoRocket and the
   * operation will succeed. The number of chunks that could not be merged
   * can be obtained with {@link SearchResult#getUnmergedChunks()}. The
   * caller can then decide whether to repeat the query with optimistic
   * merging disabled or not.</p>
   * @see SearchResult#getUnmergedChunks()
   * @param optimisticMerging {@code true} if optimistic merging should be
   * enabled, {@code false} otherwise.
   * @return a reference to this, so the API can be used fluently
   */
  public SearchParams setOptimisticMerging(boolean optimisticMerging) {
    this.optimisticMerging = optimisticMerging;
    return this;
  }

  /**
   * Get whether optimistic merging is enabled or not. See
   * {@link #setOptimisticMerging(boolean)}.
   * @return {@code true} if optimistic merging is enabled, {@code false}
   * otherwise.
   */
  public boolean isOptimisticMerging() {
    return optimisticMerging;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchParams that = (SearchParams)o;
    return Objects.equals(query, that.query) &&
      Objects.equals(layer, that.layer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, layer);
  }
}
