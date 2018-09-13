package io.georocket.query;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import io.georocket.index.generic.DefaultMetaIndexerFactory;
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
    queryCompilersArr.add(DefaultMetaIndexerFactory.class.getName());
    List<QueryCompiler> queryCompilers = new ArrayList<>();
    try {
      for (Object o : queryCompilersArr) {
          queryCompilers.add((QueryCompiler)Class.forName(o.toString()).newInstance());
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
    
    DefaultQueryCompiler compiler = new DefaultQueryCompiler();
    compiler.setQueryCompilers(queryCompilers);
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
   * Test query with key-value pair and operator: equal
   */
  @Test
  public void eq() {
    expectFixture("eq");
  }

  /**
   * Test query with key-value pair and operator: greater than
   */
  @Test
  public void gt() {
    expectFixture("gt");
  }
  
  /**
   * Test query with key-value pair and operator: greater than or equal
   */
  @Test
  public void gte() {
    expectFixture("gte");
  }

  /**
   * Test query with key-value pair and operator: less than
   */
  @Test
  public void lt() {
    expectFixture("lt");
  }
  
  /**
   * Test query with key-value pair and operator: less than or equal
   */
  @Test
  public void lte() {
    expectFixture("lte");
  }

  /**
   * Test query with logical NOT
   */
  @Test
  public void not() {
    expectFixture("not");
  }

  /**
   * Test query with logical NOT and nested EQ
   */
  @Test
  public void notEq() {
    expectFixture("not_eq");
  }

  /**
   * Test complex query
   */
  @Test
  public void complex() {
    expectFixture("complex");
  }
}
