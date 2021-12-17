package io.georocket.ogcapifeatures

import com.mitchellbosecke.pebble.PebbleEngine
import com.mitchellbosecke.pebble.loader.ClasspathLoader
import io.georocket.GeoRocket
import io.georocket.http.Endpoint
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import java.io.StringWriter
import java.nio.charset.StandardCharsets

/**
 * An endpoint that provides the WFS 3.0 OpenAPI definition
 * @author Michel Kraemer
 */
class ApiEndpoint(private val vertx: Vertx) : Endpoint {
  companion object {
    private val log = LoggerFactory.getLogger(ApiEndpoint::class.java)
  }

  private val version: String by lazy {
    val u = GeoRocket::class.java.getResource("version.dat")
    IOUtils.toString(u, StandardCharsets.UTF_8)
  }

  override suspend fun createRouter(): Router {
    val router = Router.router(vertx)

    router.get("/").handler { ctx ->
      val response = ctx.response()
      try {
        response.putHeader("content-type", "application/json")

        val engine = PebbleEngine.Builder()
          .loader(ClasspathLoader(this::class.java.classLoader))
          .newLineTrimming(false)
          .build()
        val compiledTemplate = engine.getTemplate("io/georocket/ogcapifeatures/api.yaml")

        // add template data
        val context = mutableMapOf<String, Any>()
        context["version"] = version
        context["path"] = "/ogcapifeatures"

        // render template
        val writer = StringWriter()
        compiledTemplate.evaluate(writer, context)
        val output = writer.toString()

        val api = JsonObject(Yaml().load<Map<String, Any>>(output))
        response.end(api.encodePrettily())
      } catch (t: Throwable) {
        log.error("Could not provide api description", t)
        response.setStatusCode(500).end()
      }
    }

    return router
  }
}
