package de.fhg.igd.georocket.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests {@link XMLStartElement}
 * @author Michel Kraemer
 */
public class XMLStartElementTest {
  /**
   * Test a simple element without attributes or namespaces
   */
  @Test
  public void simple() {
    XMLStartElement e = new XMLStartElement("test");
    assertEquals("<test>", e.toString());
  }
  
  /**
   * Test a simple element with a prefix
   */
  @Test
  public void prefix() {
    XMLStartElement e = new XMLStartElement("p", "test");
    assertEquals("<p:test>", e.toString());
  }
  
  /**
   * Test a simple element with attributes
   */
  @Test
  public void attrs() {
    XMLStartElement e = new XMLStartElement(null, "test", new String[] { "", "" },
        new String[] { "key1", "key2" }, new String[] { "v1", "v2" });
    assertEquals("<test key1=\"v1\" key2=\"v2\">", e.toString());
  }
  
  /**
   * Test a simple element with attributes and prefixes
   */
  @Test
  public void attrsWithPrefixes() {
    XMLStartElement e = new XMLStartElement("p", "test", new String[] { "a", "b" },
        new String[] { "key1", "key2" }, new String[] { "v1", "v2" });
    assertEquals("<p:test a:key1=\"v1\" b:key2=\"v2\">", e.toString());
  }
  
  /**
   * Test a simple element with namespaces
   */
  @Test
  public void namespaces() {
    XMLStartElement e = new XMLStartElement("p", "test",
        new String[] { "p1", "p2" }, new String[] { "http://example.com", "https://example.com" });
    assertEquals("<p:test xmlns:p1=\"http://example.com\" xmlns:p2=\"https://example.com\">", e.toString());
  }
  
  /**
   * Test a simple element with namespaces and attributes
   */
  @Test
  public void full() {
    XMLStartElement e = new XMLStartElement("p", "test",
        new String[] { "p1", "p2" }, new String[] { "http://example.com", "https://example.com" },
        new String[] { "a", "b" }, new String[] { "key1", "key2" }, new String[] { "v1", "v2" });
    assertEquals("<p:test xmlns:p1=\"http://example.com\" xmlns:p2=\"https://example.com\" "
        + "a:key1=\"v1\" b:key2=\"v2\">", e.toString());
  }
}
