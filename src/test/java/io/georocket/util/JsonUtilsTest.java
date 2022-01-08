package io.georocket.util;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonUtils}
 * @author Michel Kraemer
 */
public class JsonUtilsTest {
  /**
   * Test if a simple object can be merged
   */
  @Test
  public void simple() {
    JsonObject obj = new JsonObject()
        .put("type", "Person")
        .put("person", new JsonObject()
            .put("firstName", "Clifford")
            .put("lastName", "Thompson")
            .put("age", 40)
            .put("address", new JsonObject()
                .put("street", "First Street")
                .put("number", 6550)));
    JsonObject expected = new JsonObject()
        .put("type", "Person")
        .put("person.firstName", "Clifford")
        .put("person.lastName", "Thompson")
        .put("person.age", 40)
        .put("person.address.street", "First Street")
        .put("person.address.number", 6550);
    assertEquals(expected, JsonUtils.flatten(obj));
  }
}
