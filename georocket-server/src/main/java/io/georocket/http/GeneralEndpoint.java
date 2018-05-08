package io.georocket.http;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

import io.georocket.GeoRocket;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * An HTTP endpoint for general requests
 * @author Michel Kraemer
 */
public class GeneralEndpoint implements Endpoint {
  private final String version;

  /**
   * Create the endpoint
   */
  public GeneralEndpoint() {
    version = getVersion();
  }
  
  /**
   * @return the tool's version string
   */
  private static String getVersion() {
    URL u = GeoRocket.class.getResource("version.dat");
    String version;
    try {
      version = IOUtils.toString(u, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Could not read version information", e);
    }
    return version;
  }

  @Override
  public String getMountPoint() {
    return "/";
  }

  @Override
  public Router createRouter(Vertx vertx) {
    Router router = Router.router(vertx);
    router.get("/").handler(this::onInfo);
    router.head("/").handler(this::onPing);
    router.options("/").handler(this::onOptions);
    return router;
  }

  /**
   * Create a JSON object that can be returned by {@link #onInfo(RoutingContext)}
   * @param context the current routing context
   * @return the JSON object
   */
  protected JsonObject createInfo(RoutingContext context) {
    JsonObject o = new JsonObject()
      .put("name", "GeoRocket")
      .put("version", version)
      .put("tagline", "It's not rocket science!");
    return o;
  }
  
  /**
   * Send information about GeoRocket to the client
   * @param context the context for handling HTTP requests
   */
  protected void onInfo(RoutingContext context) {
    JsonObject o = createInfo(context);
    context.response()
      .setStatusCode(200)
      .putHeader("Content-Type", "application/json")
      .end(o.encodePrettily());
  }
  
  /**
   * Send an empty response with a status code of 200 to the client
   * @param context the context for handling HTTP requests
   */
  protected void onPing(RoutingContext context) {
    context.response()
      .setStatusCode(200)
      .end();
  }

  /**
   * Handle the HTTP options request
   * @param context the context for handling HTTP requests
   */
  protected void onOptions(RoutingContext context) {
    context.response()
      .putHeader("Allow", "GET,HEAD")
      .setStatusCode(200)
      .end();
  }
}
