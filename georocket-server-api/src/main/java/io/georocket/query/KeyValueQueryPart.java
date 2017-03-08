package io.georocket.query;

/**
 * A part of a GeoRocket query representing a key-value string
 * @author Michel Kraemer
 * @since 1.1.0
 */
public class KeyValueQueryPart implements QueryPart {
  /**
   * Specifies how two key-value pairs should be compared to each other
   */
  public enum ComparisonOperator {
    /**
     * The values must equal
     */
    EQ,

    /**
     * The value of this key-value pair must be greater than the other one
     */
    GT,

    /**
     * The value of this key-value pair must be greater than or equal to the
     * other one
     */
    GTE,

    /**
     * The value of this key-value pair must be less than the other one
     */
    LT,

    /**
     * The value of this key-value pair must be less than or equal to the
     * other one
     */
    LTE
  }

  private final String key;
  private final String value;
  private final ComparisonOperator comparator;

  /**
   * Creates a new query part
   * @param key the key of the property to compare to
   * @param value the value to compare to
   * @param comparator the used comparator
   */
  public KeyValueQueryPart(String key, String value,
      ComparisonOperator comparator) {
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
   * Get the comparison operator
   * @return the operator
   */
  public ComparisonOperator getComparisonOperator() {
    return comparator;
  }
}
