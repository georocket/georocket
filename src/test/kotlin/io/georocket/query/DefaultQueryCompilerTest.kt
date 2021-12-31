package io.georocket.query

import io.georocket.index.generic.DefaultMetaIndexerFactory
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert
import org.junit.Test

/**
 * Test [DefaultQueryCompiler]
 * @author Michel Kraemer
 */
class DefaultQueryCompilerTest {
  private fun expectFixture(fixture: String) {
    val fixtureStr = this.javaClass.getResource("fixtures/$fixture.json")!!.readText()
    val fixtureObj = JsonObject(fixtureStr)

    val query = fixtureObj.getString("query")
    val path = fixtureObj.getString("path")
    val expected = fixtureObj.getJsonObject("expected")
    val queryCompilersArr = fixtureObj.getJsonArray("queryCompilers", JsonArray())
    queryCompilersArr.add(DefaultMetaIndexerFactory::class.java.name)

    val queryCompilers = queryCompilersArr.map {
      Class.forName(it.toString()).getDeclaredConstructor().newInstance() as QueryCompiler }

    val compiler = DefaultQueryCompiler(queryCompilers)
    val compiledQuery = compiler.compileQuery(query, path)

    if (expected != compiledQuery) {
      println(Json.encodePrettily(compiledQuery))
    }

    Assert.assertEquals(expected, compiledQuery)
  }

  /**
   * Test query against an address
   */
  @Test
  fun address() {
    expectFixture("address")
  }

  /**
   * Test query with a single string
   */
  @Test
  fun string() {
    expectFixture("string")
  }

  /**
   * Test query with a single string and a path
   */
  @Test
  fun stringWithPath() {
    expectFixture("string_with_path")
  }

  /**
   * Test query with a single string and the root path `/`
   */
  @Test
  fun stringWithRootPath() {
    expectFixture("string_with_root_path")
  }

  /**
   * Test if two strings are implicitly combined using logical OR
   */
  @Test
  fun implicitOr() {
    expectFixture("implicit_or")
  }

  /**
   * Test query with a bounding box
   */
  @Test
  fun boundingBox() {
    expectFixture("bounding_box")
  }

  /**
   * Test query with a bounding box and a string
   */
  @Test
  fun boundingBoxOrString() {
    expectFixture("bounding_box_or_string")
  }

  /**
   * Test query with logical AND
   */
  @Test
  fun and() {
    expectFixture("and")
  }

  /**
   * Test query with key-value pair and operator: equal
   */
  @Test
  fun eq() {
    expectFixture("eq")
  }

  /**
   * Test query with key-value pair and operator: equal (a number)
   */
  @Test
  fun eqNumber() {
    expectFixture("eq_number")
  }

  /**
   * Test query with key-value pair and operator: greater than
   */
  @Test
  fun gt() {
    expectFixture("gt")
  }

  /**
   * Test query with key-value pair and operator: greater than (a string)
   */
  @Test
  fun gtString() {
    expectFixture("gt_string")
  }

  /**
   * Test query with key-value pair and operator: greater than or equal
   */
  @Test
  fun gte() {
    expectFixture("gte")
  }

  /**
   * Test query with key-value pair and operator: less than
   */
  @Test
  fun lt() {
    expectFixture("lt")
  }

  /**
   * Test query with key-value pair and operator: less than or equal
   */
  @Test
  fun lte() {
    expectFixture("lte")
  }

  /**
   * Test query with logical NOT
   */
  @Test
  operator fun not() {
    expectFixture("not")
  }

  /**
   * Test query with logical NOT and multiple operands
   */
  @Test
  fun notOr() {
    expectFixture("not_or")
  }

  /**
   * Test query with logical NOT and nested EQ
   */
  @Test
  fun notEq() {
    expectFixture("not_eq")
  }

  /**
   * Test complex query
   */
  @Test
  fun complex() {
    expectFixture("complex")
  }
}
