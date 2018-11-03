package io.georocket.http;

import io.georocket.constants.AddressConstants;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import static io.georocket.http.Endpoint.fail;

/**
 * An HTTP endpoint for accessing the information about tasks maintained by
 * the {@link io.georocket.tasks.TaskVerticle}
 * @author Michel Kraemer
 */
public class TaskEndpoint implements Endpoint {
  private Vertx vertx;

  @Override
  public String getMountPoint() {
    return "/tasks";
  }

  @Override
  public Router createRouter(Vertx vertx) {
    this.vertx = vertx;

    Router router = Router.router(vertx);
    router.get("/").handler(this::onGetAll);
    router.get("/:id").handler(this::onGetByCorrelationId);
    router.options("/").handler(this::onOptions);

    return router;
  }

  /**
   * Return all tasks
   * @param context the context for handling HTTP requests
   */
  private void onGetAll(RoutingContext context) {
    onGet(context, AddressConstants.TASK_GET_ALL, null);
  }

  /**
   * Return all tasks belonging to a given correlation ID
   * @param context the context for handling HTTP requests
   */
  private void onGetByCorrelationId(RoutingContext context) {
    String correlationId = context.pathParam("id");
    onGet(context, AddressConstants.TASK_GET_BY_CORRELATION_ID, correlationId);
  }

  /**
   * Helper method to generate a response containing task information
   * @param context the context for handling HTTP requests
   * @param address the address to send a request to
   * @param correlationId the correlation ID to send to the address
   */
  private void onGet(RoutingContext context, String address, String correlationId) {
    vertx.eventBus().<JsonObject>send(address, correlationId, ar -> {
      if (ar.failed()) {
        fail(context.response(), ar.cause());
      } else {
        context.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(ar.result().body().encode());
      }
    });
  }

  /**
   * Handle the HTTP options request
   * @param context the context for handling HTTP requests
   */
  protected void onOptions(RoutingContext context) {
    context.response()
      .putHeader("Allow", "GET")
      .setStatusCode(200)
      .end();
  }
}
