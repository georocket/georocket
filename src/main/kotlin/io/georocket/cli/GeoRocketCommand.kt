package io.georocket.cli

import de.undercouch.underline.Command
import de.undercouch.underline.InputReader
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.OptionGroup
import de.undercouch.underline.OptionIntrospector
import de.undercouch.underline.OptionIntrospector.ID
import de.undercouch.underline.OptionParser
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.io.PrintWriter

abstract class GeoRocketCommand : Command {
  private val options: OptionGroup<ID> = OptionIntrospector.introspect(javaClass)
  protected val vertx: Vertx by lazy { Vertx.currentContext().owner() }
  protected val config: JsonObject by lazy { Vertx.currentContext().config() }

  /**
   * The command's name displayed in the help
   */
  abstract val usageName: String

  /**
   * The command description that should be displayed in the help
   */
  abstract val usageDescription: String

  /**
   * `true` if the command's help should be displayed
   */
  @set:OptionDesc(longName = "help", shortName = "h",
    description = "display this help and exit", priority = 9000)
  var displayHelp: Boolean = false

  @Deprecated("Use suspend version of this method!", replaceWith = ReplaceWith("coRun(args, `in`, out)"))
  override fun run(args: Array<String>, `in`: InputReader, out: PrintWriter): Int {
    throw RuntimeException("Use suspend version of this method!")
  }

  suspend fun coRun(args: Array<String>, reader: InputReader, writer: PrintWriter): Int {
    val unknownArgs = OptionIntrospector.hasUnknownArguments(javaClass)
    val parsedOptions = OptionParser.parse(args, options,
      if (unknownArgs) OptionIntrospector.DEFAULT_ID else null)
    OptionIntrospector.evaluate(parsedOptions.values, this)

    if (displayHelp) {
      usage()
      return 0
    }

    if (!checkArguments()) {
      return 1
    }

    return doRun(parsedOptions.remainingArgs, reader, writer)
  }

  abstract suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
    writer: PrintWriter): Int

  /**
   * Outputs an error message
   * @param msg the message
   */
  protected fun error(msg: String?) {
    System.err.println("georocket: $msg")
  }

  /**
   * Prints out usage information
   */
  protected fun usage() {
    var name = "georocket"

    val footnotes: String? = if (options.commands.isNotEmpty()) {
      "Use `$name help <command>' to read about a specific command."
    } else {
      null
    }

    if (usageName.isNotEmpty()) {
      name += " $usageName"
    }

    val unknownArguments = OptionIntrospector.getUnknownArgumentName(javaClass)
    OptionParser.usage(name, usageDescription, options, unknownArguments,
      footnotes, PrintWriter(System.out, true))
  }

  open fun checkArguments(): Boolean {
    // nothing to check by default. subclasses may override
    return true
  }
}
