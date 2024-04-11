package io.georocket.util

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * Test [formatDuration] and [formatDurationShort]
 */
class DurationFormatTest {
  /**
   * Test [formatDuration]
   */
  @Test
  fun format() {
    assertEquals("0s", 0L.formatDuration())
    assertEquals("0.001s", 1L.formatDuration())
    assertEquals("0.010s", 10L.formatDuration())
    assertEquals("0.123s", 123L.formatDuration())
    assertEquals("1s", 1000L.formatDuration())
    assertEquals("1.024s", 1024L.formatDuration())
    assertEquals("1m", 60000L.formatDuration())
    assertEquals("1m 1.024s", (60000 + 1024L).formatDuration())
    assertEquals("1h", (60000 * 60L).formatDuration())
    assertEquals("1d", (60000 * 60 * 24L).formatDuration())
    assertEquals("400d", (60000 * 60 * 24 * 400L).formatDuration())
    assertEquals("400d 1h 10m 35.464s", (60000 * 60 * 24 * 400L + 4235464).formatDuration())
  }

  /**
   * Test [formatDurationShort]
   */
  @Test
  fun formatShort() {
    assertEquals("00:00:00", 0L.formatDurationShort())
    assertEquals("00:00:00", 1L.formatDurationShort())
    assertEquals("00:00:00", 10L.formatDurationShort())
    assertEquals("00:00:00", 123L.formatDurationShort())
    assertEquals("00:00:01", 1000L.formatDurationShort())
    assertEquals("00:00:01", 1024L.formatDurationShort())
    assertEquals("00:01:00", 60000L.formatDurationShort())
    assertEquals("00:01:01", (60000 + 1024L).formatDurationShort())
    assertEquals("01:00:00", (60000 * 60L).formatDurationShort())
    assertEquals("24:00:00", (60000 * 60 * 24L).formatDurationShort())
    assertEquals("9600:00:00", (60000 * 60 * 24 * 400L).formatDurationShort())
    assertEquals("9601:10:35", (60000 * 60 * 24 * 400L + 4235464).formatDurationShort())
  }
}
