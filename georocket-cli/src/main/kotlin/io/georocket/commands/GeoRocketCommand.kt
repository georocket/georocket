package io.georocket.commands

import de.undercouch.underline.Command
import de.undercouch.underline.InputReader
import io.vertx.core.Handler
import java.io.PrintWriter

/**
 * An interface for all GeoRocket commands
 */
interface GeoRocketCommand : Command {
  /**
   * The command's name displayed in the help
   */
  val usageName: String

  /**
   * The command description that should be displayed in the help
   */
  val usageDescription: String

  /**
   * Checks the provided arguments
   * @return `true` if all arguments are OK, `false` otherwise
   */
  fun checkArguments(): Boolean

  /**
   * Runs the command
   * @param remainingArgs arguments that have not been parsed yet, can
   * be forwarded to sub-commands
   * @param i a stream from which user input can be read
   * @param o a stream to write the output to
   * @return the command's exit code
   */
  suspend fun doRun(remainingArgs: Array<String>, i: InputReader, o: PrintWriter): Int
}
