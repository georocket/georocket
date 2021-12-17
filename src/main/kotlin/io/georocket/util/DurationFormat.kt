package io.georocket.util

import net.time4j.Duration.Formatter
import java.time.Duration

private const val PATTERN = "[#################Y'y ']" +
    "[#################M'mo '][#################W'w ']" +
    "[#################D'd '][#################h'h ']" +
    "[#################m'm '][#################s[.fff]'s']"

private val FORMATTER = Formatter.ofPattern(PATTERN)

/**
 * Convert the given number of milliseconds to a human-readable string
 */
fun Long.formatDuration(): String {
  val r = FORMATTER.format(Duration.ofMillis(this))
  return if (r.isEmpty()) "0s" else r.trim()
}

/**
 * Calculate the time elapsed since the given UNIX time and then convert
 * it to a human-readable string
 */
fun Long.formatUntilNow(): String =
    (System.currentTimeMillis() - this).formatDuration()

/**
 * Convert the given number of milliseconds to a short human-readable string
 */
fun Long.formatDurationShort(): String {
  val seconds = this / 1000
  val minutes = seconds / 60
  val hours = minutes / 60
  return String.format("%02d:%02d:%02d", hours, minutes % 60, seconds % 60)
}

/**
 * Calculate the time elapsed since the given UNIX time and then convert
 * it to a short human-readable string
 */
fun Long.formatUntilNowShort() =
    (System.currentTimeMillis() - this).formatDurationShort()
