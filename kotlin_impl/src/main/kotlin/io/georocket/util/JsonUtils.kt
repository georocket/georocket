package io.georocket.util

import io.vertx.core.json.JsonObject

/**
 * Utility functions to manipulate JSON objects and arrays
 * @author Michel Kraemer
 */
object JsonUtils {
  /**
   * Recursively flattens a hierarchy of JSON objects. It combines keys
   * of parents and their children by concatening them with a dot. For
   * example, consider the following object:
   * <pre>
   * {
   *   "type": "Person",
   *   "person": {
   *     "firstName": "Clifford",
   *     "lastName": "Thompson",
   *     "age": 40,
   *     "address": {
   *       "street": "First Street",
   *       "number": 6550
   *     }
   *   }
   * }
   * </pre>
   *
   * This object will be flattened to the following one:
   * <pre>
   * {
   *   "type": "Person",
   *   "person.firstName": "Clifford",
   *   "person.lastName": "Thompson",
   *   "person.age": 40,
   *   "person.address.street": "First Street",
   *   "person.address.number": 6550
   * }
   * </pre>
   * @param obj the object to flatten
   * @return the flattened object
   */
  fun flatten(obj: JsonObject): JsonObject {
    val result = JsonObject()
    for (key in obj.fieldNames()) {
      val value = obj.getValue(key)
      if (value is JsonObject) {
        var obj2 = value
        obj2 = flatten(obj2)
        for (key2 in obj2.fieldNames()) {
          result.put("$key.$key2", obj2.getValue(key2))
        }
      } else {
        result.put(key, value)
      }
    }
    return result
  }
}
