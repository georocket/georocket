package io.georocket.index.postgresql

import io.georocket.query.All
import io.georocket.query.And
import io.georocket.query.Compare
import io.georocket.query.Contains
import io.georocket.query.ElemMatch
import io.georocket.query.GeoIntersects
import io.georocket.query.IndexQuery
import io.georocket.query.Not
import io.georocket.query.Or
import io.georocket.query.QueryPart.ComparisonOperator.EQ
import io.georocket.query.QueryPart.ComparisonOperator.GT
import io.georocket.query.QueryPart.ComparisonOperator.GTE
import io.georocket.query.QueryPart.ComparisonOperator.LT
import io.georocket.query.QueryPart.ComparisonOperator.LTE
import io.georocket.query.StartsWith
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf

object PostgreSQLQueryTranslator {
  private fun escapeLikeExpression(expr: String): String {
    return expr.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
  }

  private fun checkField(field: String) {
    if (!field.matches("""[a-zA-Z0-9]+""".toRegex())) {
      throw IllegalArgumentException("Illegal field name: `$field'")
    }
  }

  private fun appendField(jsonField: String, field: String, result: StringBuilder) {
    val parts = field.split(".")
    checkField(jsonField)
    for (p in parts) {
      checkField(p)
    }
    val cf = parts.joinToString(separator = "'->'", prefix = "'", postfix = "'")
    result.append("${jsonField}->${cf}")
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

  private fun translate(jsonField: String, query: IndexQuery, result: StringBuilder,
      params: MutableList<Any>) {
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
        appendField(jsonField, query.field, result)
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
        appendField(jsonField, query.arrayField, result)
        result.append(" ? ")
        appendParam(query.value, result, params)
      }

      is ElemMatch -> {
        if (query.operands.all { it.op == EQ }) {
          // shortcut
          appendField(jsonField, query.arrayField, result)
          result.append(" @> ")
          val obj = jsonObjectOf()
          for (op in query.operands) {
            obj.put(op.field, op.value)
          }
          appendParam(jsonArrayOf(obj), result, params)
        } else {
          // Heads up: This query may be very slow. Perhaps, there are ways to
          // improve the performance. See this Stackoverflow answer (and the
          // related links at the end of the answer) for more information:
          // https://stackoverflow.com/a/49542329/309962
          result.append("EXISTS (SELECT FROM jsonb_array_elements(")
          appendField(jsonField, query.arrayField, result)
          result.append(") a WHERE ")
          for ((i, op) in query.operands.withIndex()) {
            if (i > 0) {
              result.append(" AND ")
            }
            translate("a", op, result, params)
          }
          result.append(")")
        }
      }

      is GeoIntersects -> {
        result.append("ST_Intersects(ST_GeomFromGeoJSON(")
        appendField(jsonField, query.field, result)
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
        appendField(jsonField, query.field, result)
        result.append("#>> '{}' LIKE ")
        appendParam(escapeLikeExpression(query.prefix) + "%", result, params)
      }
    }
  }
}
