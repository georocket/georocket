package io.georocket.util;

import static io.georocket.util.PathUtils.addTrailingSlash;
import static io.georocket.util.PathUtils.isAbsolute;
import static io.georocket.util.PathUtils.join;
import static io.georocket.util.PathUtils.normalize;
import static io.georocket.util.PathUtils.removeLeadingSlash;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test {@link PathUtils}
 * @author Andrej Sajenko
 */
public class PathUtilsTest {
  /**
   * Test join with preceding and descending slashes
   */
  @Test
  public void plainJoin() {
    assertEquals("abc", join("abc"));
    assertEquals("aa/bb", join("aa", "bb"));
    assertEquals("/aa/bb", join("/aa", "bb"));
    assertEquals("/aa/bb", join("/aa", "/bb"));
    assertEquals("/aa/bb/", join("/aa", "/bb/"));
  }

  /**
   * Test join with null and empty arguments
   */
  @Test
  public void emptyArgumentsJoin() {
    assertEquals("", join());
    assertEquals("aa", join("aa", null));
    assertEquals("aa/bb", join("aa", "", "bb"));
  }

  /**
   * Test join normalization
   */
  @Test
  public void normalizeJoin() {
    assertEquals("bb", join("./aa", "../bb"));
    assertEquals("aa/bb", join("./aa", "./bb"));
  }
  
  /**
   * Test normalize on double slashes
   */
  @Test
  public void normalizeNormalizedPaths() {
    assertEquals("abc", normalize("abc"));
    assertEquals("/abc/abc", normalize("/abc/abc"));
  }

  /**
   * Test normalize on double slashes
   */
  @Test
  public void testNormalize() {
    assertEquals("/abc", normalize("//abc"));
    assertEquals("/abc/abc/", normalize("//abc//abc//"));
    assertEquals("/abc/", normalize("/abc/"));
  }

  /**
   * Test normalize with relative in out paths.
   */
  @Test
  public void relativeInOutNormalize() {
    assertEquals("abc", normalize("./abc"));
    assertEquals("../abc", normalize("../abc"));
    assertEquals("../../abc", normalize("../../abc"));
    assertEquals("bb", normalize("./abc/../bb"));
    assertEquals("", normalize("./abc/../"));
  }

  /**
   * Test the absolute path check.
   */
  @Test
  public void testIsAbsolute() {
    assertTrue(isAbsolute("/absolute/path"));
    assertFalse(isAbsolute("relative/path"));
    assertFalse(isAbsolute(""));
  }

  /**
   * Remove leading slash
   */
  @Test
  public void testRemoveLeadingSlash() {
    assertEquals("abc", removeLeadingSlash("/abc"));
    assertEquals("abc/abc", removeLeadingSlash("/abc/abc"));
    String pathWithoutLeadingSlash = "abc/abc";
    assertEquals(pathWithoutLeadingSlash, removeLeadingSlash(pathWithoutLeadingSlash));
  }

  /**
   * Add trailing slash
   */
  @Test
  public void testAddTrailingSlash() {
    assertEquals("abc/", addTrailingSlash("abc"));
    assertEquals("/abc/abc/", addTrailingSlash("/abc/abc"));
    assertEquals("/abc/abc/", addTrailingSlash("/abc/abc/"));
    assertEquals("/", addTrailingSlash(""));
  }
}
