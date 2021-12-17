package io.georocket

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import de.undercouch.underline.CommandDesc
import de.undercouch.underline.CommandDescList
import de.undercouch.underline.InputReader
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.OptionParserException
import de.undercouch.underline.StandardInputReader
import io.georocket.cli.DeleteCommand
import io.georocket.cli.GeoRocketCommand
import io.georocket.cli.HelpCommand
import io.georocket.cli.ImportCommand
import io.georocket.cli.PropertyCommand
import io.georocket.cli.SearchCommand
import io.georocket.cli.ServerCommand
import io.georocket.cli.TagCommand
import io.georocket.constants.ConfigConstants
import io.georocket.tasks.TaskRegistry
import io.georocket.util.JsonUtils
import io.georocket.util.io.PrintWriteStream
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.core.streams.WriteStream
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.text.StringEscapeUtils
import org.fusesource.jansi.AnsiConsole
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import java.io.BufferedOutputStream
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import kotlin.system.exitProcess

private val log = LoggerFactory.getLogger(Main::class.java)
private lateinit var geoRocketHome: File

class Main : GeoRocketCommand() {
  override val usageName = "" // the tool's name will be prepended
  override val usageDescription = "Command-line interface for GeoRocket"

  @set:OptionDesc(longName = "version", shortName = "V",
    description = "output version information and exit",
    priority = 9999)
  var displayVersion: Boolean = false

  @set:CommandDescList(
    CommandDesc(longName = "import",
      description = "import one or more files into GeoRocket",
      command = ImportCommand::class),
    CommandDesc(longName = "property",
      description = "update properties of existing chunks in GeoRocket",
      command = PropertyCommand::class),
    CommandDesc(longName = "tag",
      description = "update tags of existing chunks in GeoRocket",
      command = TagCommand::class),
    CommandDesc(longName = "search",
      description = "search the GeoRocket data store",
      command = SearchCommand::class),
    CommandDesc(longName = "delete",
      description = "delete from the GeoRocket data store",
      command = DeleteCommand::class),
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

    // initialize task registry
    TaskRegistry.init(config)

    // register Jackson Kotlin module
    DatabindCodec.mapper().registerKotlinModule()
    DatabindCodec.prettyMapper().registerKotlinModule()

    // start CLI
    try {
      val out = PrintWriteStream(BufferedOutputStream(System.out, 65535))
      val exitCode = coRun(args, StandardInputReader(), out)
      System.out.flush()
      AnsiConsole.systemUninstall()
      exitProcess(exitCode)
    } catch (e: OptionParserException) {
      error(e.message)
      AnsiConsole.systemUninstall()
      exitProcess(1)
    }
  }

  private fun configureConsoleLogger() {
    val context = LoggerFactory.getILoggerFactory() as LoggerContext

    val level = config.getString(ConfigConstants.LOGS_LEVEL,
      ConfigConstants.DEFAULT_LOGS_LEVEL)

    val xml = """
      <configuration>
        <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
          <encoder class="ch.qos.logback.core.encoder.LayoutWrappingEncoder">
            <layout class="ch.qos.logback.classic.PatternLayout">
                <pattern>%d{HH:mm:ss} %-5level %logger{36} - %msg%n</pattern>
            </layout>
          </encoder>
        </appender>
        <root level="${StringEscapeUtils.escapeXml11(level)}">
          <appender-ref ref="STDOUT" />
        </root>
      </configuration>
    """

    val configurator = JoranConfigurator()
    configurator.context = context
    configurator.doConfigure(xml.byteInputStream())
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>): Int {
    if (displayVersion) {
      println("georocket $version")
      return 0
    }

    if (command is ServerCommand) {
      configureConsoleLogger()
    }

    val logConfig = config.getBoolean(ConfigConstants.LOG_CONFIG, false)
    if (logConfig) {
      log.info("""
      Configuration:
      ${config.encodePrettily()}
      """.trimIndent())
    }

    if (System.getenv("GEOROCKET_HOME") == null) {
      log.info("Environment variable GEOROCKET_HOME not set. Using current " +
          "working directory.")
    }
    log.info("Using GeoRocket home $geoRocketHome")

    // if there are no commands print usage and exit
    if (command == null) {
      usage()
      return 0
    }

    return command!!.coRun(remainingArgs, reader, out)
  }
}

class MainVerticle(private val args: Array<String>) : CoroutineVerticle() {
  override suspend fun start() {
    Main().start(args)
  }
}

/**
 * Match every environment variable against the config keys from
 * [ConfigConstants.getConfigKeys] and save the found values using
 * the config key in the config object. The method is equivalent to calling
 * [overwriteWithEnvironmentVariables]
 */
private fun overwriteWithEnvironmentVariables(conf: JsonObject) {
  overwriteWithEnvironmentVariables(conf, System.getenv())
}

/**
 * Match every environment variable against the config keys from
 * [ConfigConstants.getConfigKeys] and save the found values using
 * the config key in the config object.
 */
private fun overwriteWithEnvironmentVariables(conf: JsonObject, env: Map<String, String>) {
  val names = ConfigConstants.getConfigKeys()
    .associateBy { s -> s.uppercase().replace(".", "_") }
  env.forEach { (key, v) ->
    val name = names[key.uppercase()]
    if (name != null) {
      val yaml = Yaml()
      val newVal = yaml.load<Any>(v)
      conf.put(name, newVal)
    }
  }
}

/**
 * Replace configuration variables in a string
 */
private fun replaceConfVariables(str: String): String {
  return str.replace("\$GEOROCKET_HOME", geoRocketHome.absolutePath)
}

/**
 * Recursively replace configuration variables in an array
 */
private fun replaceConfVariables(arr: JsonArray): JsonArray {
  val result = JsonArray()
  for (o in arr) {
    val ro = when (o) {
      is JsonObject -> replaceConfVariables(o)
      is JsonArray -> replaceConfVariables(o)
      is String -> replaceConfVariables(o)
      else -> o
    }
    result.add(ro)
  }
  return result
}

/**
 * Recursively replace configuration variables in an object
 */
private fun replaceConfVariables(obj: JsonObject): JsonObject {
  val result = obj.copy()

  for (key in result.map.keys) {
    when (val value = result.getValue(key)) {
      is JsonObject -> result.put(key, replaceConfVariables(value))
      is JsonArray -> result.put(key, replaceConfVariables(value))
      is String -> result.put(key, replaceConfVariables(value))
    }
  }

  return result
}

/**
 * Amend the `logback.xml` file from the classpath with the configuration
 * properties found in the [conf] object and then configure the log framework
 */
fun configureLogging(conf: JsonObject) {
  val context = LoggerFactory.getILoggerFactory() as LoggerContext

  val level = conf.getString(ConfigConstants.LOGS_LEVEL,
    ConfigConstants.DEFAULT_LOGS_LEVEL)
  if (level != "TRACE" && level != "DEBUG" && level != "INFO" &&
    level != "WARN" && level != "ERROR" && level != "OFF") {
    throw IllegalArgumentException("Configuration item " +
        "`${ConfigConstants.LOGS_LEVEL}` must be one of `TRACE', `DEBUG', " +
        "`INFO', `WARN', `ERROR', `OFF'.")
  }

  val xml = StringBuilder("<configuration>")

  val mainEnabled = conf.getBoolean(ConfigConstants.LOGS_ENABLED, true)
  if (mainEnabled) {
    val mainLogFile = conf.getString(ConfigConstants.LOGS_LOGFILE, "logs/georocket.log")
    val dot = mainLogFile.lastIndexOf('.')

    val encoder = """
      <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
        <pattern>%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n</pattern>
      </encoder>
    """

    val rolloverEnabled = conf.getBoolean(ConfigConstants.LOGS_DAILYROLLOVER_ENABLED, true)
    if (rolloverEnabled) {
      val rolloverFilePattern = if (dot > 0) {
        mainLogFile.substring(0, dot) + ".%d{yyyy-MM-dd}" + mainLogFile.substring(dot)
      } else {
        "$mainLogFile.%d{yyyy-MM-dd}"
      }
      val rolloverMaxDays = conf.getInteger(ConfigConstants.LOGS_DAILYROLLOVER_MAXDAYS, 7)
      val rolloverMaxSize = conf.getLong(ConfigConstants.LOGS_DAILYROLLOVER_MAXSIZE, 104857600)

      xml.append("""
        <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
            $encoder
            <file>${StringEscapeUtils.escapeXml11(mainLogFile)}</file>
            <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
                <!-- daily rollover -->
                <fileNamePattern>${StringEscapeUtils.escapeXml11(rolloverFilePattern)}</fileNamePattern>
                <!-- keep n days' worth of history capped at a total size -->
                <maxHistory>$rolloverMaxDays</maxHistory>
                <totalSizeCap>$rolloverMaxSize</totalSizeCap>
            </rollingPolicy>
        </appender>
      """)
    } else {
      xml.append("""
        <appender name="FILE" class="ch.qos.logback.core.FileAppender">
            $encoder
            <file>${StringEscapeUtils.escapeXml11(mainLogFile)}</file>
        </appender>
      """)
    }
  }

  xml.append("""<root level="${StringEscapeUtils.escapeXml11(level)}">""")
  if (mainEnabled) {
    xml.append("""<appender-ref ref="FILE" />""")
  }
  xml.append("""
      </root>
    </configuration>
  """)

  val configurator = JoranConfigurator()
  configurator.context = context
  configurator.doConfigure(xml.toString().byteInputStream())
}

/**
 * Load the GeoRocket configuration
 */
private fun loadGeoRocketConfiguration(): JsonObject {
  var geoRocketHomeStr = System.getenv("GEOROCKET_HOME")
  if (geoRocketHomeStr == null) {
    geoRocketHomeStr = File(".").absolutePath
  }

  geoRocketHome = File(geoRocketHomeStr).canonicalFile

  // load configuration file
  val confDir = File(geoRocketHome, "conf")
  var confFile = File(confDir, "georocket.yaml")
  if (!confFile.exists()) {
    confFile = File(confDir, "georocket.yml")
    if (!confFile.exists()) {
      confFile = File(confDir, "georocket.json")
    }
  }

  val confFileStr = FileUtils.readFileToString(confFile, "UTF-8")
  var conf: JsonObject = if (confFile.name.endsWith(".json")) {
    JsonObject(confFileStr)
  } else {
    val yaml = Yaml()
    @Suppress("UNCHECKED_CAST")
    val m = yaml.loadAs(confFileStr, HashMap::class.java) as Map<String, Any>
    JsonUtils.flatten(JsonObject(m))
  }

  // set default configuration values
  conf.put(ConfigConstants.HOME, "\$GEOROCKET_HOME")
  if (!conf.containsKey(ConfigConstants.STORAGE_FILE_PATH)) {
    conf.put(ConfigConstants.STORAGE_FILE_PATH, "\$GEOROCKET_HOME/storage")
  }

  // replace variables in config
  conf = replaceConfVariables(conf)
  overwriteWithEnvironmentVariables(conf)

  configureLogging(conf)

  return conf
}

/**
 * Run the GeoRocket command-line interface
 * @param args the command line arguments
 */
suspend fun main(args: Array<String>) {
  val options = try {
    val conf = loadGeoRocketConfiguration()
    deploymentOptionsOf(config = conf)
  } catch (ex: IOException) {
    System.err.println("Invalid georocket home")
    ex.printStackTrace()
    exitProcess(1)
  } catch (ex: DecodeException) {
    System.err.println("Failed to decode the GeoRocket (JSON) configuration")
    ex.printStackTrace()
    exitProcess(1)
  }

  val vertx = Vertx.vertx()
  vertx.deployVerticle(MainVerticle(args), options).await()
}
