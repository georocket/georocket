package io.georocket

import io.georocket.constants.ConfigConstants
import io.georocket.http.Endpoint
import io.georocket.index.IndexerVerticle
import io.georocket.index.MetadataVerticle
import io.georocket.tasks.TaskVerticle
import io.georocket.util.FilteredServiceLoader
import io.georocket.util.JsonUtils
import io.georocket.util.SizeFormat
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Verticle
import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.core.net.PemKeyCertOptions
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.kotlin.core.http.httpServerOptionsOf
import io.vertx.rx.java.RxHelper
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.yaml.snakeyaml.Yaml
import rx.Completable
import rx.Observable
import rx.Single
import rx.plugins.RxJavaHooks
import java.io.File
import java.io.IOException
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import kotlin.system.exitProcess

private val log = LoggerFactory.getLogger(GeoRocket::class.java)
private lateinit var geoRocketHome: File

/**
 * GeoRocket - A high-performance database for geospatial files
 * @author Michel Kraemer
 */
class GeoRocket : AbstractVerticle() {
  /**
   * Deploy a new verticle with the standard configuration of this instance
   * @param cls the class of the verticle class to deploy
   * @return a single that will carry the verticle's deployment id
   */
  private fun deployVerticle(cls: Class<out Verticle?>): Single<String> {
    return Single.defer {
      val observable = RxHelper.observableFuture<String>()
      val options = DeploymentOptions().setConfig(config())
      vertx.deployVerticle(cls.name, options, observable.toHandler())
      observable.toSingle()
    }
  }

  /**
   * Deploys all verticles from GeoRocket extensions (registered through Java
   * Service Provider Interface)
   * @return a completable that completes when all verticles have been deployed
   */
  private fun deployExtensionVerticles(): Completable {
    return Completable.defer {
      val options = DeploymentOptions().setConfig(config())
      Observable.from(FilteredServiceLoader.load(ExtensionVerticle::class.java))
          .flatMap { verticle: ExtensionVerticle? ->
            val observable = RxHelper.observableFuture<String>()
            vertx.deployVerticle(verticle, options, observable.toHandler())
            observable
          }
          .toCompletable()
    }
  }

  /**
   * Deploy the indexer verticle
   * @return a single that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  private fun deployIndexer(): Single<String> {
    return deployVerticle(IndexerVerticle::class.java)
  }

  /**
   * Deploy the importer verticle
   * @return a single that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  private fun deployImporter(): Single<String> {
    return deployVerticle(ImporterVerticle::class.java)
  }

  /**
   * Deploy the metadata verticle
   * @return a single that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  private fun deployMetadata(): Single<String> {
    return deployVerticle(MetadataVerticle::class.java)
  }

  /**
   * Deploy the task verticle
   * @return a single that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  private fun deployTask(): Single<String> {
    return deployVerticle(TaskVerticle::class.java)
  }

  /**
   * Deploy the http server.
   * @return a single that will complete when the http server was started.
   */
  private fun deployHttpServer(): Single<HttpServer> {
    val host = config().getString(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST)
    val port = config().getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT)
    return try {
      val router = createRouter()
      val serverOptions = createHttpServerOptions()
      val server = vertx.createHttpServer(serverOptions)
      val observable = RxHelper.observableFuture<HttpServer>()
      server.requestHandler(router).listen(port, host, observable.toHandler())
      observable.toSingle()
    } catch (t: Throwable) {
      Single.error(t)
    }
  }

  /**
   * Create and configure a [CorsHandler]
   * @return the [CorsHandler]
   */
  private fun createCorsHandler(): CorsHandler {
    val allowedOrigin = config().getString(
        ConfigConstants.HTTP_CORS_ALLOW_ORIGIN, "$.") // match nothing by default
    val corsHandler = CorsHandler.create(allowedOrigin)

    // configure whether the Access-Control-Allow-Credentials should be returned
    if (config().getBoolean(ConfigConstants.HTTP_CORS_ALLOW_CREDENTIALS, false)) {
      corsHandler.allowCredentials(true)
    }

    // configured allowed headers
    val allowHeaders = config().getValue(ConfigConstants.HTTP_CORS_ALLOW_HEADERS)
    when {
      allowHeaders is String ->
        corsHandler.allowedHeader(allowHeaders)
      allowHeaders is JsonArray ->
        corsHandler.allowedHeaders(allowHeaders.map { it as String }.toSet())
      allowHeaders != null ->
        throw IllegalArgumentException(ConfigConstants.HTTP_CORS_ALLOW_HEADERS +
            " must either be a string or an array.")
    }

    // configured allowed methods
    val allowMethods = config().getValue(ConfigConstants.HTTP_CORS_ALLOW_METHODS)
    when {
      allowMethods is String ->
        corsHandler.allowedMethod(HttpMethod.valueOf(allowMethods))
      allowMethods is JsonArray ->
        corsHandler.allowedMethods(allowMethods.map { it as String }
            .map { HttpMethod.valueOf(it) }.toSet())
      allowMethods != null ->
        throw IllegalArgumentException(ConfigConstants.HTTP_CORS_ALLOW_METHODS +
            " must either be a string or an array.")
    }

    // configured exposed headers
    val exposeHeaders = config().getValue(ConfigConstants.HTTP_CORS_EXPOSE_HEADERS)
    when {
      exposeHeaders is String ->
        corsHandler.exposedHeader(exposeHeaders)
      exposeHeaders is JsonArray ->
        corsHandler.exposedHeaders(exposeHeaders.map { it as String}.toSet())
      exposeHeaders != null ->
        throw IllegalArgumentException(ConfigConstants.HTTP_CORS_EXPOSE_HEADERS +
            " must either be a string or an array.")
    }

    // configure max age in seconds
    val maxAge = config().getInteger(ConfigConstants.HTTP_CORS_MAX_AGE, -1)
    corsHandler.maxAgeSeconds(maxAge)
    return corsHandler
  }

  /**
   * Create a [Router] and add routes for `/store/` to it
   */
  private fun createRouter(): Router {
    val router = Router.router(vertx)

    val corsEnable = config().getBoolean(ConfigConstants.HTTP_CORS_ENABLE, false)
    if (corsEnable) {
      router.route().handler(createCorsHandler())
    }

    for (ep in FilteredServiceLoader.load(Endpoint::class.java)) {
      router.mountSubRouter(ep.mountPoint, ep.createRouter(vertx))
    }

    router.route().handler { ctx: RoutingContext ->
      val reason = "The endpoint ${ctx.request().path()} does not exist"
      ctx.response()
          .setStatusCode(404)
          .end(ServerAPIException.toJson("endpoint_not_found", reason).toString())
    }

    return router
  }

  /**
   * Create an [HttpServerOptions] object and modify it according to the
   * configuration
   */
  private fun createHttpServerOptions(): HttpServerOptions {
    val compress = config().getBoolean(ConfigConstants.HTTP_COMPRESS, true)
    val serverOptions = httpServerOptionsOf(compressionSupported = compress)

    val ssl = config().getBoolean(ConfigConstants.HTTP_SSL, false)
    if (ssl) {
      serverOptions.isSsl = ssl
      val certPath = config().getString(ConfigConstants.HTTP_CERT_PATH, null)
      val keyPath = config().getString(ConfigConstants.HTTP_KEY_PATH, null)
      val pemKeyCertOptions = PemKeyCertOptions()
          .setCertPath(certPath)
          .setKeyPath(keyPath)
      serverOptions.pemKeyCertOptions = pemKeyCertOptions
    }

    val alpn = config().getBoolean(ConfigConstants.HTTP_ALPN, false)
    if (alpn) {
      if (!ssl) {
        log.warn("ALPN is enabled but SSL is not! In order for ALPN to work " +
            "correctly, SSL is required.")
      }
      serverOptions.isUseAlpn = alpn
    }

    return serverOptions
  }

  override fun start(startFuture: Future<Void>) {
    log.info("Launching GeoRocket ${getVersion()} ...")
    deployExtensionVerticles()
        .doOnCompleted {
          vertx.eventBus().publish(ExtensionVerticle.EXTENSION_VERTICLE_ADDRESS,
              JsonObject().put("type", ExtensionVerticle.MESSAGE_ON_INIT))
        }
        .andThen(deployTask()
            .flatMap { deployIndexer() }
            .flatMap { deployImporter() }
            .flatMap { deployMetadata() }
            .flatMap { deployHttpServer() }
        )
        .subscribe({
          vertx.eventBus().publish(ExtensionVerticle.EXTENSION_VERTICLE_ADDRESS,
              JsonObject().put("type", ExtensionVerticle.MESSAGE_POST_INIT))
          log.info("GeoRocket launched successfully.")
          startFuture.complete()
        }) { cause: Throwable -> startFuture.fail(cause) }
  }

  /**
   * Return the tool's version string
   */
  private fun getVersion(): String {
    val u = GeoRocket::class.java.getResource("version.dat")
    return try {
      IOUtils.toString(u, StandardCharsets.UTF_8)
    } catch (e: IOException) {
      throw RuntimeException("Could not read version information", e)
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
 * Set default configuration values
 */
private fun setDefaultConf(conf: JsonObject) {
  conf.put(ConfigConstants.HOME, "\$GEOROCKET_HOME")
  if (!conf.containsKey(ConfigConstants.STORAGE_FILE_PATH)) {
    conf.put(ConfigConstants.STORAGE_FILE_PATH, "\$GEOROCKET_HOME/storage")
  }
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
  val conf: JsonObject = if (confFile.name.endsWith(".json")) {
    JsonObject(confFileStr)
  } else {
    val yaml = Yaml()
    @Suppress("UNCHECKED_CAST")
    val m = yaml.loadAs(confFileStr, HashMap::class.java) as Map<String, Any>
    JsonUtils.flatten(JsonObject(m))
  }

  // set default configuration values
  setDefaultConf(conf)

  // replace variables in config
  replaceConfVariables(conf)
  overwriteWithEnvironmentVariables(conf)
  return conf
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
fun overwriteWithEnvironmentVariables(conf: JsonObject, env: Map<String, String>) {
  val names = ConfigConstants.getConfigKeys()
      .map { s -> s.toUpperCase().replace(".", "_") to s }
      .toMap()
  env.forEach { (key, v) ->
    val name = names[key.toUpperCase()]
    if (name != null) {
      val yaml = Yaml()
      val newVal = yaml.load<Any>(v)
      conf.put(name, newVal)
    }
  }
}

/**
 * Run the server
 */
fun main() {
  // print banner
  try {
    val u = GeoRocket::class.java.getResource("georocket_banner.txt")
    val banner = IOUtils.toString(u, StandardCharsets.UTF_8)
    println(banner)
  } catch (e: IOException) {
    // ignore
  }

  val vertx = Vertx.vertx()

  // register schedulers that run Rx operations on the Vert.x event bus
  RxJavaHooks.setOnComputationScheduler { RxHelper.scheduler(vertx) }
  RxJavaHooks.setOnIOScheduler { RxHelper.blockingScheduler(vertx) }
  RxJavaHooks.setOnNewThreadScheduler { RxHelper.scheduler(vertx) }

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
  vertx.deployVerticle(GeoRocket::class.java.name, options) { ar ->
    if (ar.failed()) {
      log.fatal("Could not deploy GeoRocket")
      ar.cause().printStackTrace()
      exitProcess(1)
    }
  }
}
