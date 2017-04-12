package io.georocket.http;

import io.georocket.constants.AddressConstants;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Arrays;

/**
 * An HTTP endpoint handling requests related to the chunk tags
 * @author Tim Hellhake
 */
public class TagsEndpoint implements Endpoint {
  private final Vertx vertx;

  /**
   * Create the endpoint
   * @param vertx the Vert.x instance
   */
  public TagsEndpoint(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Router createRouter() {
    Router router = Router.router(vertx);
    router.put("/*").handler(this::onPut);
    router.delete("/*").handler(this::onDelete);
    return router;
  }

  /**
   * Handles the HTTP PUT request
   * @param context the routing context
   */
  private void onPut(RoutingContext context) {
    onUpdate(context, "append");
  }

  /**
   * Handles the HTTP DELETE request
   * @param context the routing context
   */
  private void onDelete(RoutingContext context) {
    onUpdate(context, "remove");
  }

  /**
   * Update the tags of a chunk
   * @param context the routing context
   * @param action the action to apply, either set or delete
   */
  private void onUpdate(RoutingContext context, String action) {
    HttpServerResponse response = context.response();
    HttpServerRequest request = context.request();
    String path = Endpoint.getEndpointPath(context);
    String search = request.getParam("search");
    String tags = request.getParam("tags");
    JsonArray updates;

    if (tags == null || tags.trim().length() == 0) {
      updates = new JsonArray();
    } else {
      updates = new JsonArray(Arrays.asList(tags.split(",")));
    }

    JsonObject msg = new JsonObject()
      .put("path", path)
      .put("search", search)
      .put("action", action)
      .put("target", "tags")
      .put("updates", updates);

    vertx.eventBus().send(AddressConstants.INDEXER_UPDATE, msg, ar -> {
      if (ar.succeeded()) {
        response.setStatusCode(204).end();
      } else {
        Endpoint.fail(response, ar.cause());
      }
    });
  }
}
