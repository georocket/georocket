package io.georocket.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * <p>Convert data sizes to human-readable strings. Used by all commands that
 * output sizes, so the output always looks the same.</p>
 * <p>Code has been adapted from
 * <a href="https://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc">https://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc</a>.</p>
 * @author Michel Kraemer
 */
public class SizeFormat {
  private static final String[] UNITS = new String[] {
    "B", "KB", "MB", "GB", "TB", "PB", "EB"
  };
  private static final DecimalFormat FORMATTER = new DecimalFormat("#,##0.#",
      DecimalFormatSymbols.getInstance(Locale.ENGLISH));

  /**
   * Convert the given data size to a human-readable string
   * @param size the data size
   * @return the human-readable string
   */
  public static String format(long size) {
    if (size <= 0) {
      return "0 B";
    }
    int d = (int)(Math.log10(size) / Math.log10(1024));
    return FORMATTER.format(size / Math.pow(1024, d)) + " " + UNITS[d];
  }
}
