package io.georocket.http;

import io.vertx.ext.web.Router;

/**
 * An HTTP endpoint
 * @author Michel Kraemer
 */
public interface Endpoint {
  /**
   * Create a router that handles HTTP requests for this endpoint
   * @return the router
   */
  Router createRouter();
}
