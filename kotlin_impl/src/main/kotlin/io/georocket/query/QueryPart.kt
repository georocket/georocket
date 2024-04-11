package io.georocket.query

/**
 * A part of a GeoRocket query
 * @author Michel Kraemer
 */
sealed interface QueryPart {
  val value: Any
  val key: String?
  val comparisonOperator: ComparisonOperator?

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

/**
 * A part of a GeoRocket query representing a search string
 * @author Michel Kraemer
 */
data class StringQueryPart(override val value: String, override val key: String? = null,
  override val comparisonOperator: QueryPart.ComparisonOperator? = null) :
  QueryPart

/**
 * A part of a GeoRocket query representing a long number
 * @author Michel Kraemer
 */
data class LongQueryPart(override val value: Long, override val key: String? = null,
  override val comparisonOperator: QueryPart.ComparisonOperator? = null) :
  QueryPart

/**
 * A part of a GeoRocket query representing a double number
 * @author Michel Kraemer
 */
data class DoubleQueryPart(override val value: Double, override val key: String? = null,
  override val comparisonOperator: QueryPart.ComparisonOperator? = null) :
  QueryPart
