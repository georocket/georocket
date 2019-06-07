package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.OptionGroup
import de.undercouch.underline.OptionIntrospector
import de.undercouch.underline.OptionIntrospector.ID
import de.undercouch.underline.OptionParser
import io.georocket.ConfigConstants
import io.georocket.client.GeoRocketClient
import io.georocket.util.coroutines.awaitBlockingConcurrent
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.runBlocking
import java.io.PrintWriter

/**
 * Abstract base class for all GeoRocket commands
 */
abstract class AbstractGeoRocketCommand : GeoRocketCommand {
  private val options: OptionGroup<ID> = OptionIntrospector.introspect(javaClass)
  protected val vertx: Vertx by lazy { Vertx.currentContext().owner() }
  protected val config: JsonObject by lazy { Vertx.currentContext().config() }

  /**
   * `true` if the command's help should be displayed
   */
  @set:OptionDesc(longName = "help", shortName = "h",
      description = "display this help and exit", priority = 9000)
  var displayHelp: Boolean = false

  /**
   * Outputs an error message
   * @param msg the message
   */
  protected fun error(msg: String?) {
    System.err.println("georocket: $msg")
  }

  suspend fun runAwait(args: Array<String>, i: InputReader, o: PrintWriter): Int {
    return awaitBlockingConcurrent { run(args, i, o) }
  }

  override fun run(args: Array<String>, i: InputReader, o: PrintWriter): Int {
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

    return runBlocking(vertx.dispatcher()) {
      doRun(parsedOptions.remainingArgs, i, o)
    }
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

  override fun checkArguments(): Boolean {
    // nothing to check by default. subclasses may override
    return true
  }

  /**
   * Create a new GeoRocket client
   */
  protected fun createClient(): GeoRocketClient {
    val host = config.getString(ConfigConstants.HOST)
    val port = config.getInteger(ConfigConstants.PORT)
    return GeoRocketClient(host, port, vertx)
  }
}
