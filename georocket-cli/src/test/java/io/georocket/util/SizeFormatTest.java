package io.georocket.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link SizeFormat}
 * @author Michel Kraemer
 */
public class SizeFormatTest {
  /**
   * Test if data sizes can be converted to human-readable strings
   */
  @Test
  public void format() {
    assertEquals("0 B", SizeFormat.format(-1));
    assertEquals("0 B", SizeFormat.format(0));
    assertEquals("100 B", SizeFormat.format(100));
    assertEquals("1 KB", SizeFormat.format(1024));
    assertEquals("1.1 KB", SizeFormat.format(1024 + 100));
    assertEquals("1.2 KB", SizeFormat.format(1024 + 200));
    assertEquals("1.3 KB", SizeFormat.format(1024 + 260));
    assertEquals("1 MB", SizeFormat.format(1024 * 1024));
    assertEquals("2.5 MB", SizeFormat.format(1024 * 1024 * 2 + 1024 * 500));
    assertEquals("1 GB", SizeFormat.format(1024 * 1024 * 1024));
    assertEquals("1 TB", SizeFormat.format(1024L * 1024 * 1024 * 1024));
    assertEquals("1 PB", SizeFormat.format(1024L * 1024 * 1024 * 1024 * 1024));
    assertEquals("1 EB", SizeFormat.format(1024L * 1024 * 1024 * 1024 * 1024 * 1024));
    assertEquals("8 EB", SizeFormat.format(Long.MAX_VALUE));
  }
}
