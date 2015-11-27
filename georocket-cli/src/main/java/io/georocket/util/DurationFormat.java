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
  private static final String PATTERN = "[-#################Y'y ']"
      + "[-#################M'mo '][-#################W'w ']"
      + "[-#################D'd '][-#################h'h ']"
      + "[-#################m'm '][-#################s[.fff]'s']";
  
  private static final Formatter<IsoUnit> FORMATTER = Formatter.ofPattern(PATTERN);
  
  /**
   * Convert the given number of milliseconds to a human-readable string
   * @param millis the milliseconds
   * @return the string
   */
  public static String format(long millis) {
    return FORMATTER.format(Duration.ofMillis(millis));
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
}
