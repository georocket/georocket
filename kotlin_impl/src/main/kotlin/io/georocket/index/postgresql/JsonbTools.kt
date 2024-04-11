/**
 * @author Tobias Dorra
 * Utility functions for generating (postgres) SQL queries involving the jsonb type.
 */
package io.georocket.index.postgresql

import io.georocket.query.Compare
import io.georocket.query.QueryPart.ComparisonOperator.*


private fun checkField(field: String) {
  if (!field.matches("""[a-zA-Z0-9]+""".toRegex())) {
    throw IllegalArgumentException("Illegal field name: `$field'")
  }
}

private fun scalarToSafeJson(value: Any): String {
  return when (value) {
    is String -> {
      // aggressively escape anything that is not alphanumeric
      // to rule out the possibility for injections of any kind
      val escaped = value.map { char ->
        val c = char.toString()
        if (c.matches("[a-zA-Z0-9]".toRegex())) {
          c
        } else {
          val hex = String.format("%04X", char.code)
          "\\u$hex"
        }
      }.joinToString("")
      "\"$escaped\""
    }
    is Number -> {
      value.toString()
    }
    is Boolean -> if (value) "true" else "false"
    else -> throw IllegalArgumentException("must be a scalar value (Number, String or Boolean)")
  }
}

fun fieldJsonValue(jsonField: String, field: String): String {
  val parts = field.split(".")
  checkField(jsonField)
  for (p in parts) {
    checkField(p)
  }
  val cf = parts.joinToString(separator = "'->'", prefix = "'", postfix = "'")
  return "${jsonField}->${cf}"
}

fun fieldStringValue(jsonField: String, field: String): String {
  val parts = field.split(".").toMutableList()
  checkField(jsonField)
  for (p in parts) {
    checkField(p)
  }
  val stringField = parts.removeLastOrNull() ?: return "$jsonField #>> '{}'"
  return if (parts.isNotEmpty()) {
    val path = parts.joinToString(separator = "'->'", prefix = "'", postfix = "'")
    "$jsonField->$path->>'$stringField'"
  } else {
    "$jsonField->>'$stringField'"
  }
}

/**
 * Builds a jsonpath for an array-of-objects to a field of the array element(s) that match the given conditions.
 *
 * Example:
 * jsonPathToArrayElementField(listOf(Compare("key", "iata_code", ComparisonOperator::EQ)), "value")
 * returns the jsonpath to the field "value" of the array element(s) with key="iata_code":
 * 'strict $[*] ? (@.key == "iata_code") . value'
 * In the following json value, this jsonpath would match the value "FRA":
 * [
 *  {"key": "scalerank", "value": 3},
 *  {"key": "featureclass", "value": "Airport"},
 *  {"key": "iata_code", "value": "FRA"}
 * ]
 */
fun jsonPathToArrayElementField(elementConditions: List<Compare>, field: String): String {
  val jsonpath = java.lang.StringBuilder()
  jsonpath.append("strict $[*]")
  for (cond in elementConditions) {
    val cmp = when (cond.op) {
      EQ -> "=="
      GT -> ">"
      GTE -> ">="
      LT -> "<"
      LTE -> "<="
    }
    jsonpath.append(" ? (@. ${scalarToSafeJson(cond.field)} $cmp ${scalarToSafeJson(cond.value)})")
  }
  jsonpath.append(". ${scalarToSafeJson(field)}")
  return jsonpath.toString()
}
