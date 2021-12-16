package io.georocket.commands.console

import io.georocket.util.SizeFormat
import io.georocket.util.formatDurationShort
import io.vertx.core.Vertx
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.fusesource.jansi.Ansi.ansi
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import java.io.Closeable
import java.io.IOException
import kotlin.math.roundToLong

class ImportProgressRenderer(private val vertx: Vertx) : Closeable {
  companion object {
    /**
     * Default refresh interval in milliseconds
     */
    private const val DEFAULT_INTERVAL = 200L

    /**
     * Slower refresh interval in milliseconds (for dumb terminals)
     */
    private const val DEFAULT_INTERVAL_SLOW = 1000L

    /**
     * Minimum terminal width. If the determined value is lower than this one
     * it will be set to [DEFAULT_TERMINAL_WIDTH]
     */
    private const val MIN_TERMINAL_WIDTH = 10

    /**
     * Default terminal width (if the actual one could not be determined)
     */
    private const val DEFAULT_TERMINAL_WIDTH = 80
  }

  /**
   * ID of the refresh timer
   */
  private val timerId: Long

  /**
   * The interval of the refresh timer
   */
  private val interval: Long

  /**
   * The determined terminal width
   */
  private val terminalWidth: Int

  /**
   * The time when the renderer was constructed
   */
  private val startTime = System.currentTimeMillis()

  /**
   * The name of the file currently being imported
   */
  private var filename = "Importing"

  /**
   * The total number of files to import
   */
  var totalFiles = 0

  /**
   * The index of the file currently being imported
   */
  var index = 0

  /**
   * The total size (in bytes) of the files to import
   */
  var totalSize: Long = 0

  /**
   * The size (in bytes) of the file currently being imported
   */
  var size: Long = 0

  /**
   * The number of bytes imported of the current file
   */
  var current: Long = 0

  /**
   * The total number of bytes imported
   */
  private var totalProgress: Long = 0

  /**
   * The value of [.totalProgress] when [.render] was
   * last called
   */
  private var lastTotalProgress: Long = 0

  /**
   * The last rendered output
   */
  private var lastRenderedOutput = ""

  /**
   * A sliding window to calculate the mean upload rate
   */
  private val rateAvg = DescriptiveStatistics(50)

  init {
    // get capabilities of the terminal
    val (terminalWidth, dumb) = try {
      TerminalBuilder.terminal().use { terminal ->
        val w = if (terminal.width < MIN_TERMINAL_WIDTH) {
          DEFAULT_TERMINAL_WIDTH
        } else {
          terminal.width
        }
        w to (Terminal.TYPE_DUMB.equals(terminal.type, true) ||
            Terminal.TYPE_DUMB_COLOR.equals(terminal.type, true))
      }
    } catch (e: IOException) {
      DEFAULT_TERMINAL_WIDTH to true
    }
    this.terminalWidth = terminalWidth

    // determine refresh interval
    interval = if (dumb) {
      DEFAULT_INTERVAL_SLOW
    } else {
      DEFAULT_INTERVAL
    }

    // render once and then start periodic timer
    render(false)
    timerId = vertx.setPeriodic(interval) { render(true) }
  }

  /**
   * Dispose this renderer and do not print progress anymore
   */
  override fun close() {
    vertx.cancelTimer(timerId)
    render(true)
  }

  /**
   * Limit a [builder] to the given [maxLength]
   */
  private fun limitToLength(builder: StringBuilder, maxLength: Int) {
    if (builder.length > maxLength) {
      builder.delete(maxLength, builder.length)
    }
  }

  /**
   * Renders the progress
   * @param resetCursor `true` if the console cursor should be reset before the
   * progress is rendered
   */
  private fun render(resetCursor: Boolean) {
    // calculate upload rate
    val currentTotalProgress = totalProgress + current
    rateAvg.addValue((currentTotalProgress - lastTotalProgress).toDouble())
    val rate = (rateAvg.sum / (rateAvg.n / (1000.0 / interval))).roundToLong()
    lastTotalProgress = currentTotalProgress

    // create first line consisting of filename and index
    val line1 = StringBuilder()
    line1.append(filename)

    if (index > 0 && totalFiles > 0) {
      line1.append(" (")
          .append(index)
          .append("/")
          .append(totalFiles)
          .append(")")
    }

    limitToLength(line1, terminalWidth - 4)

    // create second line displaying the progress in bytes
    val line2 = StringBuilder()
    line2.append("file ")
        .append(SizeFormat.format(current))
    if (size > 0 && totalSize > 0) {
      line2.append("/")
          .append(SizeFormat.format(size))
    }
    line2.append("  total ")
        .append(SizeFormat.format(currentTotalProgress))
    if (size > 0 && totalSize > 0) {
      line2.append("/")
          .append(SizeFormat.format(totalSize))
    }
    line2.append("  ")
        .append(SizeFormat.format(rate))
        .append("/s")

    limitToLength(line2, terminalWidth)

    // create third line indicating the elapsed time and the ETA
    val elapsed = System.currentTimeMillis() - startTime
    val line3 = StringBuilder()
    line3.append("time ")
        .append(elapsed.formatDurationShort())
    if (rate > 0) {
      val remaining = elapsed + (totalSize - currentTotalProgress) / rate * 1000
      line3.append("/")
          .append(remaining.formatDurationShort())
    }

    limitToLength(line3, terminalWidth)

    // output lines (if they have changed)
    val ansi = ansi()
    if (resetCursor) {
      ansi.cursorUpLine(3)
    }
    ansi
        .eraseLine().a(line1).a(" ...").newline()
        .eraseLine().a(line2).newline()
        .eraseLine().a(line3).newline()

    val output = ansi.toString()
    if (lastRenderedOutput != output) {
      print(output)
      System.out.flush()
    }
    lastRenderedOutput = output
  }

  /**
   * Set the [name] of the file currently being imported and reset the
   * current file size and progress
   */
  fun startNewFile(name: String) {
    totalProgress += size
    current = 0
    size = 0
    filename = name
  }
}
