package io.georocket.query;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

/**
 * Test {@link ElasticsearchQueryOptimizer}
 * @author Michel Kraemer
 */
public class ElasticsearchQueryOptimizerTest {
  private void expectFixture(String fixture) {
    URL u = this.getClass().getResource("fixtures/" + fixture + ".json");
    String fixtureStr;
    try {
      fixtureStr = IOUtils.toString(u, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    JsonObject fixtureObj = new JsonObject(fixtureStr);
    JsonObject query = fixtureObj.getJsonObject("query");
    JsonObject expected = fixtureObj.getJsonObject("expected");
    
    JsonObject optimizedQuery = ElasticsearchQueryOptimizer.INSTANCE.optimize(query);
    
    if (!expected.equals(optimizedQuery)) {
      System.out.println(Json.encodePrettily(optimizedQuery));
    }
    assertEquals(expected, optimizedQuery);
  }
  
  /**
   * Make sure a "must" array and an embedded "should" array are not merged
   */
  @Test
  public void boolMustArrShouldArr() {
    expectFixture("optimizeBoolMustArrShouldArr");
  }

  /**
   * Test if two embedded "must" clauses are merged
   */
  @Test
  public void boolMustMust() {
    expectFixture("optimizeBoolMustMust");
  }

  /**
   * Test if a "must" clause and an embedded "should" clause are embedded
   */
  @Test
  public void boolMustShould() {
    expectFixture("optimizeBoolMustShould");
  }
  
  /**
   * Make sure a "must" clause and an embedded "should" array are not merged
   */
  @Test
  public void boolMustShouldArr() {
    expectFixture("optimizeBoolMustShouldArr");
  }

  /**
   * Test if a "must" clause with one child is removed
   */
  @Test
  public void boolOneMust() {
    expectFixture("optimizeBoolOneMust");
  }

  /**
   * Test if a "should" clause with one child is removed
   */
  @Test
  public void boolOneShould() {
    expectFixture("optimizeBoolOneShould");
  }

  /**
   * Test if the "minimum_should_match" property is removed
   */
  @Test
  public void boolOneShouldMinimumMatch() {
    expectFixture("optimizeBoolOneShouldMinimumMatch");
  }

  /**
   * Test if two embedded "should" arrays are merged
   */
  @Test
  public void boolShouldArrShouldArr() {
    expectFixture("optimizeBoolShouldArrShouldArr");
  }
}
