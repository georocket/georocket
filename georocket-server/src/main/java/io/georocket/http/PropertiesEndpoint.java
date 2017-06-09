package io.georocket.http;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.AsyncCursor;
import io.georocket.storage.MetadataStore;
import io.georocket.storage.indexed.IndexedMetadataStore;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Arrays;

/**
 * An HTTP endpoint handling requests related to the chunk properties
 * @author Tim Hellhake
 */
public class PropertiesEndpoint extends AbstractEndpoint {
  private final Vertx vertx;
  private final MetadataStore store;

  /**
   * Create the endpoint
   * @param vertx the Vert.x instance
   */
  public PropertiesEndpoint(Vertx vertx) {
    this.vertx = vertx;
    this.store = new IndexedMetadataStore(vertx);
  }

  @Override
  public Router createRouter() {
    Router router = Router.router(vertx);
    router.get("/*").handler(this::onGet);
    router.put("/*").handler(this::onPut);
    router.delete("/*").handler(this::onDelete);
    return router;
  }

  /**
   * Handles the HTTP GET request
   * @param context the routing context
   */
  private void onGet(RoutingContext context) {
    HttpServerResponse response = context.response();
    HttpServerRequest request = context.request();
    String path = getEndpointPath(context);
    String search = request.getParam("search");
    String property = request.getParam("property");

    response.setChunked(true);
    response.write("[");

    store.getPropertyValues(search, path, property, ar -> {
      if (ar.succeeded()) {
        AsyncCursor<String> cursor = ar.result();
        merge(cursor, response, 0, ar2 -> {
          if (ar2.succeeded()) {
            response
              .write("]")
              .setStatusCode(200)
              .end();
          } else {
            fail(response, ar2.cause());
          }
        });
      } else {
        fail(response, ar.cause());
      }
    });
  }

  /**
   * Merge the results of the cursor to a json array
   * @param cursor the result cursor
   * @param response the http response object
   * @param count the current count of results which have been sent
   * @param handler will be called when all results have been sent
   */
  private static void merge(AsyncCursor<String> cursor, HttpServerResponse response,
    long count, Handler<AsyncResult<Long>> handler) {
    if (cursor.hasNext()) {
      cursor.next(ar -> {
        if (ar.succeeded()) {
          if (count > 0) {
            response.write(",");
          }
          response.write("\"" + ar.result() + "\"");
          merge(cursor, response, count + 1, handler);
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    } else {
      handler.handle(Future.succeededFuture(count));
    }
  }

  /**
   * Handles the HTTP PUT request
   * @param context the routing context
   */
  private void onPut(RoutingContext context) {
    onUpdate(context, "set");
  }

  /**
   * Handles the HTTP DELETE request
   * @param context the routing context
   */
  private void onDelete(RoutingContext context) {
    onUpdate(context, "remove");
  }

  /**
   * Update the properties of a chunk
   * @param context the routing context
   * @param action the action to apply, either set or delete
   */
  private void onUpdate(RoutingContext context, String action) {
    HttpServerResponse response = context.response();
    HttpServerRequest request = context.request();
    String path = getEndpointPath(context);
    String search = request.getParam("search");
    String properties = request.getParam("properties");
    JsonArray updates;

    if (properties == null || properties.trim().length() == 0) {
      updates = new JsonArray();
    } else {
      updates = new JsonArray(Arrays.asList(properties.split(",")));
    }

    JsonObject msg = new JsonObject()
      .put("path", path)
      .put("search", search)
      .put("action", action)
      .put("target", "properties")
      .put("updates", updates);

    vertx.eventBus().send(AddressConstants.METADATA_UPDATE, msg, ar -> {
      if (ar.succeeded()) {
        response.setStatusCode(204).end();
      } else {
        fail(response, ar.cause());
      }
    });
  }
}
