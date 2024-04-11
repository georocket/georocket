package io.georocket.index.postgresql

import io.georocket.query.*
import io.georocket.query.QueryPart.ComparisonOperator.*
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonArrayOf

object PostgreSQLQueryTranslator {
  private fun escapeLikeExpression(expr: String): String {
    return expr.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
  }

  private fun appendParam(value: Any, result: StringBuilder, params: MutableList<Any>) {
    params.add(value)
    result.append("$${params.size}")
  }

  fun translate(query: IndexQuery): Pair<String, List<Any>> {
    val result = StringBuilder()
    val params = mutableListOf<Any>()
    translate("data", query, result, params)
    return result.toString() to params
  }

  private fun translate(
    jsonField: String, query: IndexQuery, result: StringBuilder,
    params: MutableList<Any>
  ) {
    when (query) {
      is All -> result.append("TRUE")

      is And -> {
        result.append("(")
        for ((i, op) in query.operands.withIndex()) {
          if (i > 0) {
            result.append(" AND ")
          }
          translate(jsonField, op, result, params)
        }
        result.append(")")
      }

      is Compare -> {
        result.append(fieldJsonValue(jsonField, query.field))
        when (query.op) {
          EQ -> result.append(" = ")
          GT -> result.append(" > ")
          GTE -> result.append(" >= ")
          LT -> result.append(" < ")
          LTE -> result.append(" <= ")
        }
        appendParam(query.value, result, params)
      }

      is Contains -> {
        result.append(fieldJsonValue(jsonField, query.arrayField))
        result.append(" @> ")
        appendParam(jsonArrayOf(query.value), result, params)
      }

      is ElemMatchExists -> {
        result.append(fieldJsonValue(jsonField, query.arrayField))
        result.append(" @> ")
        appendParam(jsonArrayOf(JsonObject(query.matches.toMap())), result, params)
      }

      is ElemMatchCompare -> {
        if (query.condition.op == EQ) {
          return translate(
            jsonField, ElemMatchExists(
              query.arrayField,
              matches = query.match.toTypedArray() + (query.condition.field to query.condition.value)
            ), result,
            params
          )
        }
        result.append("jsonb_path_query_first(")
        result.append(fieldJsonValue(jsonField, query.arrayField))
        result.append(", '")
        result.append(
          jsonPathToArrayElementField(
            query.match.map { (field, value) -> Compare(field, value, EQ) },
            query.condition.field
          )
        )
        result.append("')")
        result.append(
          when (query.condition.op) {
            EQ -> "="
            GT -> ">"
            GTE -> ">="
            LT -> "<"
            LTE -> "<="
          }
        )
        appendParam(query.condition.value, result, params)
      }

      is GeoIntersects -> {
        result.append("ST_Intersects(ST_GeomFromGeoJSON(")
        result.append(fieldJsonValue(jsonField, query.field))
        result.append("), ST_GeomFromGeoJSON(")
        appendParam(query.geometry, result, params)
        result.append("::jsonb))")
      }

      is Not -> {
        result.append("NOT ")
        translate(jsonField, query.operand, result, params)
      }

      is Or -> {
        result.append("(")
        for ((i, op) in query.operands.withIndex()) {
          if (i > 0) {
            result.append(" OR ")
          }
          translate(jsonField, op, result, params)
        }
        result.append(")")
      }

      is StartsWith -> {
        result.append(fieldStringValue(jsonField, query.field))
        result.append(" LIKE ")
        appendParam(escapeLikeExpression(query.prefix) + "%", result, params)
      }
    }
  }
}
