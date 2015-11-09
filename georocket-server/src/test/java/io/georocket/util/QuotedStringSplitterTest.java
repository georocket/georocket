package io.georocket.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

/**
 * Test {@link QuotedStringSplitter}
 * @author Michel Kraemer
 */
public class QuotedStringSplitterTest {
  /**
   * Split a simple string
   */
  @Test
  public void noQuotes() {
    assertEquals(Arrays.asList("hello", "world"),
        QuotedStringSplitter.split("hello world"));
  }
  
  /**
   * Test trimming
   */
  @Test
  public void manySpaces() {
    assertEquals(Arrays.asList("hello", "world"),
        QuotedStringSplitter.split("  hello    world "));
  }
  
  /**
   * Test double quotes
   */
  @Test
  public void doubleQuotes() {
    assertEquals(Arrays.asList("hello world", "test"),
        QuotedStringSplitter.split("\"hello world\" test"));
  }
  
  /**
   * Test double quoted and trimming
   */
  @Test
  public void doubleQuotesWithSpaces() {
    assertEquals(Arrays.asList(" hello   world", "test"),
        QuotedStringSplitter.split("\" hello   world\" test"));
  }
  
  /**
   * Test single quotes
   */
  @Test
  public void singleQuotes() {
    assertEquals(Arrays.asList("hello world", "test"),
        QuotedStringSplitter.split("'hello world' test"));
  }
  
  /**
   * Test single quotes and trimming
   */
  @Test
  public void singleQuotesWithSpaces() {
    assertEquals(Arrays.asList(" hello   world", "test"),
        QuotedStringSplitter.split("' hello   world' test"));
  }
  
  /**
   * Test if a single single-quote is included
   */
  @Test
  public void singleSingleQuote() {
    assertEquals(Arrays.asList("that's", "cool"),
        QuotedStringSplitter.split("that's cool"));
  }
  
  /**
   * Test mixed single and double-quotes
   */
  @Test
  public void mixed() {
    assertEquals(Arrays.asList("'s g'", "s' ", "\"s", "s\""),
        QuotedStringSplitter.split("\"'s g'\" \"s' \" '\"s' 's\"'"));
  }
  
  /**
   * Test escaping
   */
  @Test
  public void escaped() {
    assertEquals(Arrays.asList("a'b", "a\"b", "\n"),
        QuotedStringSplitter.split("'a\\'b' \"a\\\"b\" \\n"));
  }
}
