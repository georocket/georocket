package io.georocket.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DurationFormat}
 * @author Michel Kraemer
 */
public class DurationFormatTest {
  /**
   * Test {@link DurationFormat#format(long)}
   */
  @Test
  public void format() {
    assertEquals("0s", DurationFormat.format(0));
    assertEquals("0.001s", DurationFormat.format(1));
    assertEquals("0.010s", DurationFormat.format(10));
    assertEquals("0.123s", DurationFormat.format(123));
    assertEquals("1s", DurationFormat.format(1000));
    assertEquals("1.024s", DurationFormat.format(1024));
    assertEquals("1m", DurationFormat.format(60000));
    assertEquals("1m 1.024s", DurationFormat.format(60000 + 1024));
    assertEquals("1h", DurationFormat.format(60000 * 60));
    assertEquals("1d", DurationFormat.format(60000 * 60 * 24));
    assertEquals("400d", DurationFormat.format(60000 * 60 * 24 * 400L));
    assertEquals("400d 1h 10m 35.464s", DurationFormat.format(60000 * 60 * 24 * 400L + 4235464));
  }

  /**
   * Test {@link DurationFormat#formatShort(long)}
   */
  @Test
  public void formatShort() {
    assertEquals("00:00:00", DurationFormat.formatShort(0));
    assertEquals("00:00:00", DurationFormat.formatShort(1));
    assertEquals("00:00:00", DurationFormat.formatShort(10));
    assertEquals("00:00:00", DurationFormat.formatShort(123));
    assertEquals("00:00:01", DurationFormat.formatShort(1000));
    assertEquals("00:00:01", DurationFormat.formatShort(1024));
    assertEquals("00:01:00", DurationFormat.formatShort(60000));
    assertEquals("00:01:01", DurationFormat.formatShort(60000 + 1024));
    assertEquals("01:00:00", DurationFormat.formatShort(60000 * 60));
    assertEquals("24:00:00", DurationFormat.formatShort(60000 * 60 * 24));
    assertEquals("9600:00:00", DurationFormat.formatShort(60000 * 60 * 24 * 400L));
    assertEquals("9601:10:35", DurationFormat.formatShort(60000 * 60 * 24 * 400L + 4235464));
  }
}
