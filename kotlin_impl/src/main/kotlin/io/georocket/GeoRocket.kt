package io.georocket

import io.georocket.constants.ConfigConstants
import io.georocket.http.Endpoint
import io.georocket.http.GeneralEndpoint
import io.georocket.http.StoreEndpoint
import io.georocket.http.TaskEndpoint
import io.georocket.ogcapifeatures.LandingPageEndpoint
import io.vertx.core.DeploymentOptions
import io.vertx.core.Promise
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.net.PemKeyCertOptions
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.kotlin.core.http.httpServerOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets

/**
 * GeoRocket - A high-performance database for geospatial files
 * @author Michel Kraemer
 */
class GeoRocket(private val shutdownPromise: Promise<Unit>) : CoroutineVerticle() {
  companion object {
    private val log = LoggerFactory.getLogger(GeoRocket::class.java)
  }

  private val endpoints = mutableListOf<Endpoint>()

  /**
   * The tool's version string
   */
  private val version by lazy {
    val u = GeoRocket::class.java.getResource("version.dat")
    IOUtils.toString(u, StandardCharsets.UTF_8)
  }

  /**
   * Deploy the http server.
   * @return a single that will complete when the http server was started.
   */
  private suspend fun deployHttpServer() {
    val host = config.getString(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST)
    val port = config.getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT)
    val router = createRouter()
    val serverOptions = createHttpServerOptions()
    val server = vertx.createHttpServer(serverOptions)
    server.requestHandler(router).listen(port, host).await()
  }

  /**
   * Create and configure a [CorsHandler]
   * @return the [CorsHandler]
   */
  private fun createCorsHandler(): CorsHandler {
    val allowedOrigin = config.getString(
        ConfigConstants.HTTP_CORS_ALLOW_ORIGIN, "$.") // match nothing by default
    val corsHandler = CorsHandler.create(allowedOrigin)

    // configure whether the Access-Control-Allow-Credentials should be returned
    if (config.getBoolean(ConfigConstants.HTTP_CORS_ALLOW_CREDENTIALS, false)) {
      corsHandler.allowCredentials(true)
    }

    // configured allowed headers
    val allowHeaders = config.getValue(ConfigConstants.HTTP_CORS_ALLOW_HEADERS)
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
    val allowMethods = config.getValue(ConfigConstants.HTTP_CORS_ALLOW_METHODS)
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
    val exposeHeaders = config.getValue(ConfigConstants.HTTP_CORS_EXPOSE_HEADERS)
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
    val maxAge = config.getInteger(ConfigConstants.HTTP_CORS_MAX_AGE, -1)
    corsHandler.maxAgeSeconds(maxAge)
    return corsHandler
  }

  /**
   * Create a [Router] and add routes for `/store/` to it
   */
  private suspend fun createRouter(): Router {
    val router = Router.router(vertx)

    val corsEnable = config.getBoolean(ConfigConstants.HTTP_CORS_ENABLE, false)
    if (corsEnable) {
      router.route().handler(createCorsHandler())
    }

    val ge = GeneralEndpoint(vertx)
    router.mountSubRouter("/", ge.createRouter())

    val se = StoreEndpoint(coroutineContext, vertx)
    router.mountSubRouter("/store", se.createRouter())

    val te = TaskEndpoint(coroutineContext, vertx)
    router.mountSubRouter("/tasks", te.createRouter())

    val ogc = LandingPageEndpoint(coroutineContext, vertx)
    router.mountSubRouter("/ogcapifeatures", ogc.createRouter())

    router.route().handler { ctx ->
      val reason = "The endpoint ${ctx.request().path()} does not exist"
      ctx.response()
          .setStatusCode(404)
          .end(ServerAPIException.toJson("endpoint_not_found", reason).toString())
    }

    endpoints.add(ge)
    endpoints.add(se)
    endpoints.add(te)
    endpoints.add(ogc)

    return router
  }

  /**
   * Create an [HttpServerOptions] object and modify it according to the
   * configuration
   */
  private fun createHttpServerOptions(): HttpServerOptions {
    val compress = config.getBoolean(ConfigConstants.HTTP_COMPRESS, true)
    val serverOptions = httpServerOptionsOf(compressionSupported = compress)

    val ssl = config.getBoolean(ConfigConstants.HTTP_SSL, false)
    if (ssl) {
      serverOptions.isSsl = ssl
      val certPath = config.getString(ConfigConstants.HTTP_CERT_PATH, null)
      val keyPath = config.getString(ConfigConstants.HTTP_KEY_PATH, null)
      val pemKeyCertOptions = PemKeyCertOptions()
          .setCertPath(certPath)
          .setKeyPath(keyPath)
      serverOptions.pemKeyCertOptions = pemKeyCertOptions
    }

    val alpn = config.getBoolean(ConfigConstants.HTTP_ALPN, false)
    if (alpn) {
      if (!ssl) {
        log.warn("ALPN is enabled but SSL is not! In order for ALPN to work " +
            "correctly, SSL is required.")
      }
      serverOptions.isUseAlpn = alpn
    }

    return serverOptions
  }

  override suspend fun start() {
    log.info("Launching GeoRocket $version ...")

    val options = DeploymentOptions().setConfig(config)

    // deploy verticles
    vertx.deployVerticle(ImporterVerticle(), options).await()

    // deploy HTTP server
    deployHttpServer()

    log.info("GeoRocket launched successfully.")
  }

  override suspend fun stop() {
    endpoints.forEach { it.close() }
    shutdownPromise.complete()
  }
}
