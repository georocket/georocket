package io.georocket

import de.undercouch.underline.CommandDesc
import de.undercouch.underline.CommandDescList
import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.OptionParserException
import de.undercouch.underline.StandardInputReader
import io.georocket.client.GeoRocketClient
import io.georocket.commands.AbstractGeoRocketCommand
import io.georocket.commands.ImportCommand
import io.georocket.commands.PropertyCommand
import io.georocket.commands.TagCommand
import io.georocket.util.JsonUtils
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonObject
import org.fusesource.jansi.AnsiConsole
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.io.IOException
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import kotlin.system.exitProcess

/**
 * GeoRocket command-line interface
 */
class GeoRocketCli : AbstractGeoRocketCommand() {
  companion object {
    /**
     * The tool's version string
     */
    val version = GeoRocketCli::class.java.getResource("version.dat").readText()
  }

  /**
   * GeoRocket CLI's home directory
   */
  private val geoRocketCliHome: File
  private var port: Int? = null

  override val usageName = "" // the tool's name will be prepended
  override val usageDescription = "Command-line interface for GeoRocket"

  @set:OptionDesc(longName = "host",
      description = "the name of the host where GeoRocket is running",
      argumentName = "HOST", argumentType = ArgumentType.STRING)
  var host: String? = null

  @set:OptionDesc(longName = "version", shortName = "V",
      description = "output version information and exit",
      priority = 9999)
  var displayVersion: Boolean = false

  @set:OptionDesc(longName = "conf", shortName = "c",
      description = "path to the application's configuration file",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  var confFilePath: String? = null

  @set:CommandDescList(
      CommandDesc(longName = "import",
          description = "import one or more files into GeoRocket",
          command = ImportCommand::class),
      CommandDesc(longName = "property",
          description = "update properties of existing chunks in GeoRocket",
          command = PropertyCommand::class),
      CommandDesc(longName = "tag",
          description = "update tags of existing chunks in GeoRocket",
          command = TagCommand::class)
  )
  var command: AbstractGeoRocketCommand? = null

  init {
    // get GEOROCKET_CLI_HOME
    var geoRocketCliHomeStr: String? = System.getenv("GEOROCKET_CLI_HOME")
    if (geoRocketCliHomeStr == null) {
      System.err.println("Environment variable GEOROCKET_CLI_HOME not set. " +
          "Using current working directory.")
      geoRocketCliHomeStr = File(".").absolutePath
    }

    try {
      geoRocketCliHome = File(geoRocketCliHomeStr).canonicalFile
    } catch (e: IOException) {
      System.err.println("Invalid GeoRocket home: $geoRocketCliHomeStr")
      exitProcess(1)
    }
  }

  /**
   * Set the port GeoRocket server is listening on
   * @param port the port
   */
  @OptionDesc(longName = "port",
      description = "the port GeoRocket server is listening on",
      argumentName = "PORT", argumentType = ArgumentType.STRING)
  @Suppress("UNUSED")
  fun setPort(port: String) {
    try {
      this.port = port.toInt()
    } catch (e: NumberFormatException) {
      error("invalid port: $port")
      exitProcess(1)
    }
  }

  private fun initConfig() {
    // load configuration file
    val confFile = if (confFilePath != null) {
      File(confFilePath)
    } else {
      val confDir = File(geoRocketCliHome, "conf")
      var cf = File(confDir, "georocket.yaml")
      if (!cf.exists()) {
        cf = File(confDir, "georocket.yml")
        if (!cf.exists()) {
          cf = File(confDir, "georocket.json")
        }
      }
      cf
    }

    try {
      val confFileStr = confFile.readText()
      val readConf = if (confFile.name.endsWith(".json")) {
        JsonObject(confFileStr)
      } else {
        @Suppress("UNCHECKED_CAST")
        val m = Yaml().loadAs(confFileStr, Map::class.java) as Map<String, Any>
        JsonUtils.flatten(JsonObject(m))
      }
      config.mergeIn(readConf)
    } catch (e: IOException) {
      System.err.println("Could not read config file $confFile: ${e.message}")
      exitProcess(1)
    } catch (e: DecodeException) {
      System.err.println("Invalid config file: ${e.message}")
      exitProcess(1)
    }

    // set default values
    if (!config.containsKey(ConfigConstants.HOST)) {
      config.put(ConfigConstants.HOST, GeoRocketClient.DEFAULT_HOST)
    }
    if (!config.containsKey(ConfigConstants.PORT)) {
      config.put(ConfigConstants.PORT, GeoRocketClient.DEFAULT_PORT)
    }

    // overwrite with values from command line
    if (host != null) {
      config.put(ConfigConstants.HOST, host)
    }
    if (port != null) {
      config.put(ConfigConstants.PORT, port)
    }
  }

  override suspend fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter): Int {
    if (displayVersion) {
      println("georocket $version")
      return 0
    }

    initConfig()

    // if there are no commands print usage and exit
    if (command == null) {
      usage()
      return 0
    }

    return command!!.runAwait(remainingArgs, i, o)
  }

  /**
   * Start the CLI
   * @param args the command line arguments
   */
  fun start(args: Array<String>) {
    AnsiConsole.systemInstall()

    // start CLI
    try {
      val out = PrintWriter(OutputStreamWriter(System.out, StandardCharsets.UTF_8))
      val exitCode = run(args, StandardInputReader(), out)
      out.flush()
      AnsiConsole.systemUninstall()
      exitProcess(exitCode)
    } catch (e: OptionParserException) {
      error(e.message)
      AnsiConsole.systemUninstall()
      exitProcess(1)
    }
  }
}

/**
 * Run the GeoRocket command-line interface
 * @param args the command line arguments
 */
fun main(args: Array<String>) {
  val options = VertxOptions()
  options.maxWorkerExecuteTime = Long.MAX_VALUE
  val vertx = Vertx.vertx(options)
  vertx.executeBlocking<Unit>({ f ->
    GeoRocketCli().start(args)
    f.complete()
  }, false, { ar ->
    if (ar.failed()) {
      ar.cause().printStackTrace()
      exitProcess(1)
    }
  })
}
