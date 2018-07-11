package io.georocket;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

/**
 * Base class for a verticle contributed by a GeoRocket extension. It provides
 * a default implementation of {@link AbstractVerticle#start()} that handles
 * messages sent to {@link ExtensionVerticle#EXTENSION_VERTICLE_ADDRESS} and
 * calls the respective abstract methods.
 * @author Michel Kraemer
 * @since 1.2.0
 */
public class ExtensionVerticleBase extends AbstractVerticle
    implements ExtensionVerticle {
  @Override
  public void start(Future<Void> startFuture) throws Exception {
    start();

    vertx.eventBus().<JsonObject>consumer(EXTENSION_VERTICLE_ADDRESS, msg -> {
      JsonObject b = msg.body();
      String type = b.getString("type");
      switch (type) {
        case MESSAGE_ON_INIT:
          onInit(b);
          break;

        case MESSAGE_POST_INIT:
          onPostInit(b);
          break;
      }
    });

    onPreInit(startFuture);
  }

  /**
   * This method will be called before other internal GeoRocket verticles such
   * as the importer or the indexer are started and before any endpoints are
   * mounted. This includes internal GeoRocket endpoints and those contributed
   * by extensions.
   * @param startFuture a future that should be called when the verticle has
   * been initialized
   */
  protected void onPreInit(Future<Void> startFuture) {
    onPreInit();
    startFuture.complete();
  }

  /**
   * This method will be called before other internal GeoRocket verticles such
   * as the importer or the indexer are started and before any endpoints are
   * mounted. This includes internal GeoRocket endpoints and those contributed
   * by extensions.
   */
  protected void onPreInit() {
    // empty default implementation
  }

  /**
   * This method will be called asynchronously during GeoRocket's
   * initialization process
   * @param msg initialization parameters
   */
  protected void onInit(JsonObject msg) {
    // empty default implementation
  }

  /**
   * This method will be called asynchronously at the end of GeoRocket's
   * initialization process
   * @param msg initialization parameters
   */
  protected void onPostInit(JsonObject msg) {
    // empty default implementation
  }
}
