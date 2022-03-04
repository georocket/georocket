package io.georocket.util

import io.georocket.util.SizeFormat.format
import org.junit.Assert
import org.junit.Test

/**
 * Test [SizeFormat]
 * @author Michel Kraemer
 */
class SizeFormatTest {
  /**
   * Test if data sizes can be converted to human-readable strings
   */
  @Test
  fun format() {
    Assert.assertEquals("0 B", format(-1))
    Assert.assertEquals("0 B", format(0))
    Assert.assertEquals("100 B", format(100))
    Assert.assertEquals("1 kB", format(1000))
    Assert.assertEquals("1 kB", format(1024))
    Assert.assertEquals("1.1 kB", format((1000 + 100).toLong()))
    Assert.assertEquals("1.2 kB", format((1000 + 200).toLong()))
    Assert.assertEquals("1.3 kB", format((1000 + 260).toLong()))
    Assert.assertEquals("1 MB", format((1000 * 1000).toLong()))
    Assert.assertEquals("2.5 MB", format((1000 * 1000 * 2 + 1000 * 500).toLong()))
    Assert.assertEquals("1 GB", format((1000 * 1000 * 1000).toLong()))
    Assert.assertEquals("1 TB", format(1000L * 1000 * 1000 * 1000))
    Assert.assertEquals("1 PB", format(1000L * 1000 * 1000 * 1000 * 1000))
    Assert.assertEquals("1 EB", format(1000L * 1000 * 1000 * 1000 * 1000 * 1000))
    Assert.assertEquals("9.2 EB", format(Long.MAX_VALUE))
  }
}
