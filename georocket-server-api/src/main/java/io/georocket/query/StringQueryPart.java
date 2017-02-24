package io.georocket.query;

/**
 * A part of a GeoRocket query representing a search string
 * @author Michel Kraemer
 * @since 1.1.0
 */
public class StringQueryPart implements QueryPart {
  private final String search;

  /**
   * Creates a new query part
   * @param search the search string
   */
  public StringQueryPart(String search) {
    this.search = search;
  }

  /**
   * Get the search string
   * @return the string
   */
  public String getSearchString() {
    return search;
  }
}
