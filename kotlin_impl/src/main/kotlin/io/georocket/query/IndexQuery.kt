package io.georocket.query

import io.vertx.core.json.JsonObject

sealed interface IndexQuery

object All : IndexQuery
data class And(val operands: List<IndexQuery>) : IndexQuery {
  constructor(vararg operands: IndexQuery) : this(operands.toList())
}

data class Or(val operands: List<IndexQuery>) : IndexQuery {
  constructor(vararg operands: IndexQuery) : this(operands.toList())
}

data class Not(val operand: IndexQuery) : IndexQuery
data class StartsWith(val field: String, val prefix: String) : IndexQuery
data class Compare(val field: String, val value: Any, val op: QueryPart.ComparisonOperator) : IndexQuery
data class ElemMatchExists(val arrayField: String, val matches: List<Pair<String, Any>>) : IndexQuery {
  constructor(arrayField: String, vararg matches: Pair<String, Any>) : this(arrayField, matches.asList())
}

/**
 * Looks up and compares (see [Compare]) a value in an array of nested objects.
 */
data class ElemMatchCompare(
  val arrayField: String,
  val match: List<Pair<String, Any>>,
  val condition: Compare
) : IndexQuery {
  constructor(arrayField: String, match1: Pair<String, Any>, condition: Compare) :
    this(arrayField, listOf(match1), condition)

  constructor(arrayField: String, match1: Pair<String, Any>, match2: Pair<String, Any>, condition: Compare) :
    this(arrayField, listOf(match1, match2), condition)
}

data class Contains(val arrayField: String, val value: Any) : IndexQuery
data class GeoIntersects(val field: String, val geometry: JsonObject) : IndexQuery
