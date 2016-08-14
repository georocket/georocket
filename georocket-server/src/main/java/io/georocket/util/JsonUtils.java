package io.georocket.util;

import io.vertx.core.json.JsonObject;

/**
 * Utility functions to manipulate JSON objects and arrays
 * @author Michel Kraemer
 */
public class JsonUtils {
  /**
   * <p>Recursively flattens a hierarchy of JSON objects. Combines keys
   * of parents and their children by concatening them with a dot. For
   * example, consider the following object:</p>
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
   * <p>This object will be flattened to the following one:</p>
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
  public static JsonObject flatten(JsonObject obj) {
    JsonObject result = new JsonObject();
    for (String key : obj.fieldNames()) {
      Object value = obj.getValue(key);
      if (value instanceof JsonObject) {
        JsonObject obj2 = (JsonObject)value;
        obj2 = flatten(obj2);
        for (String key2 : obj2.fieldNames()) {
          result.put(key + "." + key2, obj2.getValue(key2));
        }
      } else {
        result.put(key, value);
      }
    }
    return result;
  }
}
