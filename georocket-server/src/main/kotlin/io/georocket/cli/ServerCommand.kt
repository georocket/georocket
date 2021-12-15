package io.georocket.cli

import de.undercouch.underline.InputReader
import io.georocket.GeoRocket
import io.georocket.constants.ConfigConstants
import io.georocket.util.JsonUtils
import io.georocket.util.SizeFormat
import io.vertx.core.DeploymentOptions
import io.vertx.core.Promise
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.coroutines.await
import org.apache.commons.io.FileUtils
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.io.IOException
import java.io.PrintWriter
import java.lang.management.ManagementFactory
import kotlin.system.exitProcess

/**
 * Run GeoRocket in server mode
 */
class ServerCommand : GeoRocketCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(ServerCommand::class.java)
  }

  override val usageName = "server"
  override val usageDescription = "Run GeoRocket in server mode"

  private lateinit var geoRocketHome: File

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
   * Load the GeoRocket configuration
   */
  private fun loadGeoRocketConfiguration(): JsonObject {
    var geoRocketHomeStr = System.getenv("GEOROCKET_HOME")
    if (geoRocketHomeStr == null) {
      log.info("Environment variable GEOROCKET_HOME not set. Using current " +
          "working directory.")
      geoRocketHomeStr = File(".").absolutePath
    }

    geoRocketHome = File(geoRocketHomeStr).canonicalFile
    log.info("Using GeoRocket home $geoRocketHome")

    // load configuration file
    val confDir = File(geoRocketHome, "conf")
    var confFile = File(confDir, "georocketd.yaml")
    if (!confFile.exists()) {
      confFile = File(confDir, "georocketd.yml")
      if (!confFile.exists()) {
        confFile = File(confDir, "georocketd.json")
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

    return conf
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      writer: PrintWriter): Int {
    // print banner
    val banner = GeoRocket::class.java.getResource("georocket_banner.txt")!!.readText()
    println(banner)

    val options = DeploymentOptions()
    try {
      val conf = loadGeoRocketConfiguration()
      options.config = conf
    } catch (ex: IOException) {
      log.fatal("Invalid georocket home", ex)
      exitProcess(1)
    } catch (ex: DecodeException) {
      log.fatal("Failed to decode the GeoRocket (JSON) configuration", ex)
      exitProcess(1)
    }
    val logConfig = options.config.getBoolean(
      ConfigConstants.LOG_CONFIG, false)
    if (logConfig) {
      log.info("""
      Configuration:
      ${options.config.encodePrettily()}
      """.trimIndent())
    }

    // log memory info
    val memoryMXBean = ManagementFactory.getMemoryMXBean()
    val memoryInit = memoryMXBean.heapMemoryUsage.init
    val memoryMax = memoryMXBean.heapMemoryUsage.max
    log.info("Initial heap size: ${SizeFormat.format(memoryInit)}, " +
        "max heap size: ${SizeFormat.format(memoryMax)}")

    // deploy main verticle
    val shutdownPromise = Promise.promise<Unit>()
    try {
      vertx.deployVerticleAwait(GeoRocket(shutdownPromise), options)
    } catch (t: Throwable) {
      log.fatal("Could not deploy GeoRocket")
      t.printStackTrace()
      return 1
    }

    // wait until GeoRocket verticle has shut down
    shutdownPromise.future().await()

    return 0
  }
}
