package io.georocket.query

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * Test [String.escapeRegex]
 * @author Michel Kraemer
 */
class RegexUtilsTest {
  @Test
  fun noEscape() {
    assertEquals("Hello world", "Hello world".escapeRegex())
    assertEquals("Elvis lives!", "Elvis lives!".escapeRegex())
  }

  @Test
  fun escape() {
    assertEquals("Hello \\.\\+", "Hello .+".escapeRegex())
    assertEquals("Why\\?", "Why?".escapeRegex())
    assertEquals("This is a \\(simple\\) test\\.", "This is a (simple) test.".escapeRegex())
    assertEquals("/path/to/a/file\\.txt", "/path/to/a/file.txt".escapeRegex())
  }
}
