package io.georocket.index

import io.georocket.query.QueryPart.ComparisonOperator.EQ

/**
 * @author Tobias Dorra
 */
sealed interface DatabaseIndex {

  val name: String

  /**
   * Index for scalar values.
   * Can be used for accelerating [io.georocket.query.Compare] queries with [EQ] comparisons. (No range queries)
   */
  data class Eq(val field: String, override val name: String) : DatabaseIndex

  /**
   * Index for scalar values, with the addition of range queries compared to [DatabaseIndex.Eq].
   * Can be used for accelerating arbitrary [io.georocket.query.Compare] queries.
   */
  data class Range(val field: String, override val name: String) : DatabaseIndex

  /**
   * Index for string values.
   * Can be used for accelerating [io.georocket.query.StartsWith].
   */
  data class StartsWith(val field: String, override val name: String) : DatabaseIndex

  /**
   * Index for array values (that contain scalar values).
   * Can be used for accelerating [io.georocket.query.Contains] queries.
   */
  data class Array(val field: String, override val name: String) : DatabaseIndex

  /**
   * Index for arrays of nested objects.
   *
   * MongoDb:
   * Can be used for accelerating both [io.georocket.query.ElemMatchExists] and
   * [io.georocket.query.ElemMatchCompare] queries.
   * However, the order of the [matchedOn] fields matters:
   * The index can only accelerate queries, that use a prefix of the list of [matchedOn] fields.
   *
   * Postgres:
   * Can only be used for accelerating [io.georocket.query.ElemMatchExists].
   * For [io.georocket.query.ElemMatchCompare], an [ElemMatchCompare] index is needed.
   */
  data class ElemMatchExists(val field: String, val matchedOn: List<String>, override val name: String) : DatabaseIndex

  data class ElemMatchCompare(
    val field: String,
    val elemKeyField: String,
    val elemValueField: String,
    val keyValue: String,
    override val name: String
  ) : DatabaseIndex

  /**
   * Spatial index for accelerating [io.georocket.query.GeoIntersects] queries.
   */
  data class Geo(val field: String, override val name: String) : DatabaseIndex

}
