package io.georocket.query;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Test {@link DefaultQueryCompiler}
 * @author Michel Kraemer
 */
public class DefaultQueryCompilerTest {
  private void expectFixture(String fixture) {
    URL u = this.getClass().getResource("fixtures/" + fixture + ".json");
    String fixtureStr;
    try {
      fixtureStr = IOUtils.toString(u, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    JsonObject fixtureObj = new JsonObject(fixtureStr);
    String query = fixtureObj.getString("query");
    JsonObject expected = fixtureObj.getJsonObject("expected");
    JsonArray queryCompilersArr = fixtureObj.getJsonArray("queryCompilers", new JsonArray());
    List<QueryCompiler> queryCompilers = new ArrayList<>();
    try {
      for (Object o : queryCompilersArr) {
          queryCompilers.add((QueryCompiler)Class.forName(o.toString()).newInstance());
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
    
    QueryCompiler compiler = new DefaultQueryCompiler(queryCompilers);
    JsonObject compiledQuery = compiler.compileQuery(query);
    if (!expected.equals(compiledQuery)) {
      System.out.println(Json.encodePrettily(compiledQuery));
    }
    assertEquals(expected, compiledQuery);
  }
  
  /**
   * Test query with a single string
   */
  @Test
  public void string() {
    expectFixture("string");
  }
  
  /**
   * Test if two strings are implicitly combined using logical OR
   */
  @Test
  public void implicitOr() {
    expectFixture("implicit_or");
  }
  
  /**
   * Test query with a bounding box
   */
  @Test
  public void boundingBox() {
    expectFixture("bounding_box");
  }
  
  /**
   * Test query with a bounding box and a string
   */
  @Test
  public void boundingBoxOrString() {
    expectFixture("bounding_box_or_string");
  }
  
  /**
   * Test query with logical AND
   */
  @Test
  public void and() {
    expectFixture("and");
  }
  
  /**
   * Test complex query
   */
  @Test
  public void complex() {
    expectFixture("complex");
  }
}
