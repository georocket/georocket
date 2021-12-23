package io.georocket.cli

import io.vertx.core.Vertx
import org.fusesource.jansi.Ansi
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import java.io.Closeable
import java.io.IOException

class SpinnerRenderer(private val vertx: Vertx, private val message: String) : Closeable {
  companion object {
    // copied from https://github.com/sindresorhus/cli-spinners, released under
    // the MIT license by Sindre Sorhus
    private val frames = listOf("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")

    /**
     * Default refresh interval in milliseconds
     */
    private const val DEFAULT_INTERVAL = 75L

    /**
     * Slower refresh interval in milliseconds (for dumb terminals)
     */
    private const val DEFAULT_INTERVAL_SLOW = 1000L
  }

  /**
   * ID of the refresh timer
   */
  private val timerId: Long

  /**
   * `true` if this is a dumb terminal
   */
  private val dumb: Boolean = try {
    TerminalBuilder.terminal().use { terminal ->
      Terminal.TYPE_DUMB.equals(terminal.type, true) ||
          Terminal.TYPE_DUMB_COLOR.equals(terminal.type, true)
    }
  } catch (e: IOException) {
    true
  }

  /**
   * The character currently being rendered
   */
  private var currentFrame = 0

  init {
    // determine refresh interval
    val interval = if (dumb) {
      DEFAULT_INTERVAL_SLOW
    } else {
      DEFAULT_INTERVAL
    }

    // render once and then start periodic timer
    if (dumb) {
      print("$message ")
    }
    render()
    timerId = vertx.setPeriodic(interval) { render() }
  }

  /**
   * Dispose this renderer and do not print progress anymore
   */
  override fun close() {
    vertx.cancelTimer(timerId)
    if (dumb) {
      println()
    } else {
      val ansi = Ansi.ansi()
      print(ansi.cursorToColumn(0).eraseLine().a("\u001B[?25h"))
    }
  }

  private fun render() {
    if (dumb) {
      print(".")
    } else {
      val ansi = Ansi.ansi()
      print(ansi.a("\u001B[?25l").cursorToColumn(0).eraseLine().a("${frames[currentFrame]} $message "))
      currentFrame = (currentFrame + 1) % frames.size
    }
  }
}
