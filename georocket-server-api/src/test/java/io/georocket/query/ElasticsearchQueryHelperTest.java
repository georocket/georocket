package io.georocket.query;

import static io.georocket.query.ElasticsearchQueryHelper.boolAddShould;
import static io.georocket.query.ElasticsearchQueryHelper.boolQuery;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Unit tests for {@link ElasticsearchQueryHelper}
 * @author Michel Kraemer
 */
public class ElasticsearchQueryHelperTest {
  /**
   * Test {@link ElasticsearchQueryHelper#boolQuery()}
   */
  @Test
  public void testBoolQuery() {
    assertEquals(new JsonObject().put("bool", new JsonObject()), boolQuery());
  }
  
  /**
   * Test {@link ElasticsearchQueryHelper#boolAddShould(JsonObject, JsonObject)}
   */
  @Test
  public void testBoolAddShould() {
    JsonObject s = new JsonObject()
      .put("term", new JsonObject()
        .put("tags", "Fraunhofer IGD"));
    JsonObject q = boolQuery();
    boolAddShould(q, s);
    JsonObject expected = new JsonObject()
      .put("bool", new JsonObject()
        .put("should", s));
    assertEquals(expected, q);
  }
  
  /**
   * Test that {@link ElasticsearchQueryHelper#boolAddShould(JsonObject, JsonObject)}
   * does not add duplicated queries
   */
  @Test
  public void testBoolAddShouldNoDuplicate() {
    JsonObject s = new JsonObject()
      .put("term", new JsonObject()
        .put("tags", "Fraunhofer IGD"));
    JsonObject q = boolQuery();
    boolAddShould(q, s);
    boolAddShould(q, s);
    JsonObject expected = new JsonObject()
      .put("bool", new JsonObject()
        .put("should", s));
    assertEquals(expected, q);
  }
  
  /**
   * Test that {@link ElasticsearchQueryHelper#boolAddShould(JsonObject, JsonObject)}
   * does not add queries specified multiple times
   */
  @Test
  public void testBoolAddShouldNoMultiple() {
    JsonObject s = new JsonObject()
      .put("term", new JsonObject()
        .put("tags", "Fraunhofer IGD"));
    JsonObject s2 = new JsonObject()
      .put("term", new JsonObject()
        .put("tags", "Elvis"));
    JsonObject q = boolQuery();
    boolAddShould(q, s);
    boolAddShould(q, s2);
    boolAddShould(q, s);
    boolAddShould(q, s);
    JsonObject expected = new JsonObject()
      .put("bool", new JsonObject()
        .put("should", new JsonArray()
          .add(s)
          .add(s2)));
    assertEquals(expected, q);
  }
}
