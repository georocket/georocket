package io.georocket

import de.undercouch.underline.CommandDesc
import de.undercouch.underline.CommandDescList
import de.undercouch.underline.InputReader
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.OptionParserException
import de.undercouch.underline.StandardInputReader
import io.georocket.cli.GeoRocketCommand
import io.georocket.cli.HelpCommand
import io.georocket.cli.ServerCommand
import io.vertx.core.Vertx
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.apache.commons.io.IOUtils
import org.fusesource.jansi.AnsiConsole
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import kotlin.system.exitProcess

class Main : GeoRocketCommand() {
  override val usageName = "" // the tool's name will be prepended
  override val usageDescription = "Command-line interface for GeoRocket"

  @set:OptionDesc(longName = "version", shortName = "V",
    description = "output version information and exit",
    priority = 9999)
  var displayVersion: Boolean = false

  @set:CommandDescList(
    CommandDesc(longName = "server",
      description = "run GeoRocket in server mode",
      command = ServerCommand::class),
    CommandDesc(longName = "help",
      description = "display help for a given command",
      command = HelpCommand::class)
  )
  var command: GeoRocketCommand? = null

  /**
   * The tool's version string
   */
  private val version by lazy {
    val u = GeoRocket::class.java.getResource("version.dat")
    IOUtils.toString(u, StandardCharsets.UTF_8)
  }

  suspend fun start(args: Array<String>) {
    AnsiConsole.systemInstall()

    // start CLI
    try {
      val out = PrintWriter(OutputStreamWriter(System.out, StandardCharsets.UTF_8))
      val exitCode = coRun(args, StandardInputReader(), out)
      out.flush()
      AnsiConsole.systemUninstall()
      exitProcess(exitCode)
    } catch (e: OptionParserException) {
      error(e.message)
      AnsiConsole.systemUninstall()
      exitProcess(1)
    }
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader, writer: PrintWriter): Int {
    if (displayVersion) {
      println("georocket $version")
      return 0
    }

    // if there are no commands print usage and exit
    if (command == null) {
      usage()
      return 0
    }

    return command!!.coRun(remainingArgs, reader, writer)
  }
}

class MainVerticle(private val args: Array<String>) : CoroutineVerticle() {
  override suspend fun start() {
    Main().start(args)
  }
}

/**
 * Run the GeoRocket command-line interface
 * @param args the command line arguments
 */
suspend fun main(args: Array<String>) {
  val vertx = Vertx.vertx()
  vertx.deployVerticleAwait(MainVerticle(args))
}
