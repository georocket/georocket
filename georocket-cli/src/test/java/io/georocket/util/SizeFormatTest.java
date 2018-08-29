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
    assertEquals("1 kB", SizeFormat.format(1000));
    assertEquals("1 kB", SizeFormat.format(1024));
    assertEquals("1.1 kB", SizeFormat.format(1000 + 100));
    assertEquals("1.2 kB", SizeFormat.format(1000 + 200));
    assertEquals("1.3 kB", SizeFormat.format(1000 + 260));
    assertEquals("1 MB", SizeFormat.format(1000 * 1000));
    assertEquals("2.5 MB", SizeFormat.format(1000 * 1000 * 2 + 1000 * 500));
    assertEquals("1 GB", SizeFormat.format(1000 * 1000 * 1000));
    assertEquals("1 TB", SizeFormat.format(1000L * 1000 * 1000 * 1000));
    assertEquals("1 PB", SizeFormat.format(1000L * 1000 * 1000 * 1000 * 1000));
    assertEquals("1 EB", SizeFormat.format(1000L * 1000 * 1000 * 1000 * 1000 * 1000));
    assertEquals("9.2 EB", SizeFormat.format(Long.MAX_VALUE));
  }
}
