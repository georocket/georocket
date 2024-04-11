package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.UnknownAttributes
import io.georocket.Main
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

/**
 * Displays a command's help
 */
class HelpCommand : GeoRocketCommand() {
  override val usageName = "help"
  override val usageDescription = "Display a command's help"

  /**
   * The commands to display the help for
   */
  @set:UnknownAttributes("COMMAND")
  var commands: List<String> = ArrayList()

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>): Int {
    // simply forward commands to GeoRocketCli and append '-h'
    val cmd = Main()
    val args = commands + "-h"
    return cmd.coRun(args.toTypedArray(), reader, out)
  }
}
