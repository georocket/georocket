package io.georocket;

import io.vertx.core.Future;
import io.vertx.core.Verticle;

/**
 * <p>A verticle contributed by a GeoRocket extension.</p>
 *
 * <p>Implementations should be registered through the Java Service Provider
 * Interface (SPI). Upon startup, GeoRocket will instantiate and start
 * registered implementations and then publish messages to the event bus
 * at the address {@link #EXTENSION_VERTICLE_ADDRESS}. The messages will be
 * JSON objects with the attribute {@code type} set to either
 * {@link #MESSAGE_ON_INIT} or {@link #MESSAGE_POST_INIT}, depending on
 * whether the extension is notified during GeoRocket's initialization process
 * or at the end, respectively. The initialization process itself consists of
 * deploying GeoRocket's internal verticles such as the importer and the
 * indexer, as well as mounting all endpoints. This includes internal GeoRocket
 * endpoints and those contributed by extensions.</p>
 *
 * <p>Note: The messages are sent asynchronously. If you want to execute code
 * <em>before</em> GeoRocket's initialization process is started, just
 * implement {@link Verticle#start(Future)}.</p>
 *
 * @see ExtensionVerticleBase
 * @author Michel Kraemer
 * @since 1.2.0
 */
public interface ExtensionVerticle extends Verticle {
  /**
   * The address GeoRocket will send the initialization messages to.
   */
  String EXTENSION_VERTICLE_ADDRESS = "georocket.extension.address";

  /**
   * This message will be sent during GeoRocket's initialization process
   */
  String MESSAGE_ON_INIT = "georocket.extension.onInit";

  /**
   * This message will be sent at the end of the initialization process
   */
  String MESSAGE_POST_INIT = "georocket.extension.postInit";
}
