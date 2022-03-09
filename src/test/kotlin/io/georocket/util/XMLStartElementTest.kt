package io.georocket.util

import org.junit.Assert
import org.junit.Test

/**
 * Tests [XMLStartElement]
 * @author Michel Kraemer
 */
class XMLStartElementTest {
  /**
   * Test a simple element without attributes or namespaces
   */
  @Test
  fun simple() {
    val e = XMLStartElement(localName = "test")
    Assert.assertEquals("<test>", e.toString())
  }

  /**
   * Test a simple element with a prefix
   */
  @Test
  fun prefix() {
    val e = XMLStartElement("p", "test")
    Assert.assertEquals("<p:test>", e.toString())
  }

  /**
   * Test a simple element with attributes
   */
  @Test
  fun attrs() {
    val e = XMLStartElement(null, "test",
      attributePrefixes = listOf(null, null),
      attributeLocalNames = listOf("key1", "key2"),
      attributeValues = listOf("v1", "v2"))
    Assert.assertEquals("<test key1=\"v1\" key2=\"v2\">", e.toString())
  }

  /**
   * Test a simple element with attributes and prefixes
   */
  @Test
  fun attrsWithPrefixes() {
    val e = XMLStartElement("p", "test",
      attributePrefixes = listOf("a", "b"),
      attributeLocalNames = listOf("key1", "key2"),
      attributeValues = listOf("v1", "v2"))
    Assert.assertEquals("<p:test a:key1=\"v1\" b:key2=\"v2\">", e.toString())
  }

  /**
   * Test a simple element with namespaces
   */
  @Test
  fun namespaces() {
    val e = XMLStartElement("p", "test", listOf("p1", "p2"), listOf("http://example.com", "https://example.com"))
    Assert.assertEquals("<p:test xmlns:p1=\"http://example.com\" xmlns:p2=\"https://example.com\">", e.toString())
  }

  /**
   * Test a simple element with namespaces and attributes
   */
  @Test
  fun full() {
    val e = XMLStartElement(
      "p",
      "test",
      listOf("p1", "p2"),
      listOf("http://example.com", "https://example.com"),
      listOf("a", "b"),
      listOf("key1", "key2"),
      listOf("v1", "v2")
    )
    Assert.assertEquals(
      "<p:test xmlns:p1=\"http://example.com\" xmlns:p2=\"https://example.com\" " + "a:key1=\"v1\" b:key2=\"v2\">",
      e.toString()
    )
  }
}
