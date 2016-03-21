package io.georocket.util;

import static org.junit.Assert.*;
import static io.georocket.util.PathUtils.*;
/**
 * Test {@link PathUtils}
 * @author Andrej Sajenko
 */
public class PathUtilsTest {

  /**
   * Test join with preceding and descending slashes
   *
   * @throws Exception
   */
  @org.junit.Test
  public void plainJoin() throws Exception {
    assertEquals("abc", join("abc"));

    assertEquals("aa/bb", join("aa", "bb"));

    assertEquals("/aa/bb", join("/aa", "bb"));

    assertEquals("/aa/bb", join("/aa", "/bb"));

    assertEquals("/aa/bb/", join("/aa", "/bb/"));
  }

  /**
   * Test join with null and empty arguments
   *
   * @throws Exception
   */
  @org.junit.Test
  public void emptyArgumentsJoin() throws Exception {
    assertEquals("", join());

    assertEquals("aa", join("aa", null));

    assertEquals("aa/bb", join("aa", "", "bb"));
  }

  /**
   * Test join normalization
   *
   * @throws Exception
   */
  @org.junit.Test
  public void normalizeJoin() throws Exception {
    assertEquals("bb", join("./aa", "../bb"));

    assertEquals("aa/bb", join("./aa", "./bb"));
  }
  /**
   * Test normalize on double slashes
   *
   * @throws Exception
   */
  @org.junit.Test
  public void normalizeNormalizedPaths() throws Exception {
    assertEquals("abc", normalize("abc"));

    assertEquals("/abc/abc", normalize("/abc/abc"));
  }


  /**
   * Test normalize on double slashes
   *
   * @throws Exception
   */
  @org.junit.Test
  public void testNormalize() throws Exception {
    assertEquals("/abc", normalize("//abc"));

    assertEquals("/abc/abc/", normalize("//abc//abc//"));

    assertEquals("/abc/", normalize("/abc/"));
  }

  /**
   * Test normalize with relative in out paths.
   *
   * @throws Exception
   */
  @org.junit.Test
  public void relativeInOutNormalize() throws Exception {
    assertEquals("abc", normalize("./abc"));

    assertEquals("../abc", normalize("../abc"));

    assertEquals("../../abc", normalize("../../abc"));

    assertEquals("bb", normalize("./abc/../bb"));

    assertEquals("", normalize("./abc/../"));
  }

  /**
   * Test the absolute path check.
   *
   * @throws Exception
   */
  @org.junit.Test
  public void testIsAbsolute() throws Exception {
    assertTrue(isAbsolute("/absolute/path"));

    assertFalse(isAbsolute("relative/path"));

    assertFalse(isAbsolute(""));
  }

  /**
   * Remove leading slash
   *
   * @throws Exception
   */
  @org.junit.Test
  public void testRemoveLeadingSlash() throws Exception {
    assertEquals("abc", removeLeadingSlash("/abc"));

    assertEquals("abc/abc", removeLeadingSlash("/abc/abc"));

    String pathWithoutLeadingSlash = "abc/abc";
    assertEquals(pathWithoutLeadingSlash, removeLeadingSlash(pathWithoutLeadingSlash));
  }

  /**
   * Add trailing slash
   *
   * @throws Exception
   */
  @org.junit.Test
  public void testAddTrailingSlash() throws Exception {
    assertEquals("abc/", addTrailingSlash("abc"));

    assertEquals("/abc/abc/", addTrailingSlash("/abc/abc"));

    assertEquals("/abc/abc/", addTrailingSlash("/abc/abc/"));

    assertEquals("/", addTrailingSlash(""));
  }
}