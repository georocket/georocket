package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.UnknownAttributes
import io.georocket.GeoRocketCli
import io.vertx.core.Handler
import java.io.PrintWriter
import java.util.ArrayList

/**
 * Displays a command's help
 */
class HelpCommand : AbstractGeoRocketCommand() {
  override val usageName = "help"
  override val usageDescription = "Display a command's help"

  /**
   * The commands to display the help for
   */
  @set:UnknownAttributes("COMMAND")
  var commands: List<String> = ArrayList()

  override fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter, handler: Handler<Int>) {
    // simply forward commands to GeoRocketCli and append '-h'
    val cmd = GeoRocketCli()
    val args = commands + "-h"
    cmd.endHandler = handler
    cmd.run(args.toTypedArray(), i, o)
  }
}
