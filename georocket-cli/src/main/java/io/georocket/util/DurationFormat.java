package io.georocket.util;

import java.time.Duration;

import net.time4j.Duration.Formatter;
import net.time4j.IsoUnit;

/**
 * Convert durations to human-readable strings. Used by all commands that
 * output durations, so the output always looks the same.
 * @author Michel Kraemer
 */
public class DurationFormat {
  private static final String PATTERN = "[#################Y'y ']"
      + "[#################M'mo '][#################W'w ']"
      + "[#################D'd '][#################h'h ']"
      + "[#################m'm '][#################s[.fff]'s']";

  private static final Formatter<IsoUnit> FORMATTER = Formatter.ofPattern(PATTERN);

  /**
   * Convert the given number of milliseconds to a human-readable string
   * @param millis the milliseconds
   * @return the string
   */
  public static String format(long millis) {
    String r = FORMATTER.format(Duration.ofMillis(millis));
    if (r.isEmpty()) {
      return "0s";
    }
    return r.trim();
  }
  
  /**
   * Calculate the time elapsed since the given UNIX time and then convert
   * it to a human-readable string
   * @param startMillis the UNIX time
   * @return the string
   */
  public static String formatUntilNow(long startMillis) {
    return format(System.currentTimeMillis() - startMillis);
  }

  /**
   * Convert the given number of milliseconds to a short human-readable string
   * @param millis the milliseconds
   * @return the string
   */
  public static String formatShort(long millis) {
    long seconds = millis / 1000;
    long minutes = seconds / 60;
    long hours = minutes / 60;
    return String.format("%02d:%02d:%02d", hours, minutes % 60, seconds % 60);
  }

  /**
   * Calculate the time elapsed since the given UNIX time and then convert
   * it to a short human-readable string
   * @param startMillis the UNIX time
   * @return the string
   */
  public static String formatUntilNowShort(long startMillis) {
    return formatShort(System.currentTimeMillis() - startMillis);
  }
}
