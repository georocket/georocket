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
data class ElemMatch(val arrayField: String, val operands: List<Compare>) : IndexQuery {
  constructor(field: String, vararg operands: Compare) : this(field, operands.toList())
}
data class Contains(val arrayField: String, val value: Any) : IndexQuery
data class GeoIntersects(val field: String, val geometry: JsonObject) : IndexQuery
