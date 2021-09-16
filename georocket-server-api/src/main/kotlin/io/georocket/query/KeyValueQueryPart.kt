package io.georocket.query

/**
 * A part of a GeoRocket query representing a key-value string
 * @author Michel Kraemer
 * @since 1.1.0
 */
data class KeyValueQueryPart(val key: String, val value: String,
  val comparisonOperator: ComparisonOperator) : QueryPart {
  /**
   * Specifies how two key-value pairs should be compared to each other
   */
  enum class ComparisonOperator {
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
}
