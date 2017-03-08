package io.georocket.query;

/**
 * A part of a GeoRocket query representing a key-value string
 * @author Michel Kraemer
 * @since 1.1.0
 */
public class KeyValueQueryPart implements QueryPart {

  private final String key;
  private final String value;
  private final Comparator comparator;

  /**
   * Creates a new query part
   * @param key the key of the property to compare to
   * @param value the value to compare to
   * @param comparator the used comparator
   */
  public KeyValueQueryPart(String key, String value, Comparator comparator) {
    this.key = key;
    this.value = value;
    this.comparator = comparator;
  }

  /**
   * Get the key of the property to compare to
   * @return the key
   */
  public String getKey() {
    return key;
  }
  
  /**
   * Get the value to compare to
   * @return the value
   */
  public String getValue() {
    return value;
  }

  /**
   * Get the comparator
   * @return the comparator
   */
  public Comparator getComparator() {
    return comparator;
  }
}
