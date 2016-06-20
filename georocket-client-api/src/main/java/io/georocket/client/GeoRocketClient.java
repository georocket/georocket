package io.georocket.client;

import java.io.Closeable;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;

/**
 * Provides interface to perform operations against a GeoRocket server
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class GeoRocketClient implements Closeable {
  /**
   * Default server host: "localhost"
   */
  public static final String DEFAULT_HOST = "localhost";
  
  /**
   * Default server port: 63020
   */
  public static final int DEFAULT_PORT = 63020;
  
  /**
   * HTTP client used to connect to GeoRocket
   */
  protected final HttpClient client;
  
  /**
   * Create a client connecting to GeoRocket running on
   * {@value #DEFAULT_HOST}:{@value #DEFAULT_PORT}
   * @param vertx a Vert.x instance used to create a HTTP client
   */
  public GeoRocketClient(Vertx vertx) {
    this(DEFAULT_HOST, vertx);
  }
  
  /**
   * Create a client connecting to GeoRocket running on the
   * specified host and the default port {@value #DEFAULT_PORT}
   * @param host the GeoRocket host
   * @param vertx a Vert.x instance used to create a HTTP client
   */
  public GeoRocketClient(String host, Vertx vertx) {
    this(host, DEFAULT_PORT, vertx);
  }
  
  /**
   * Create a client connecting to GeoRocket running on the
   * specified host and port
   * @param host the GeoRocket host
   * @param port the GeoRocket port
   * @param vertx a Vert.x instance used to create a HTTP client
   */
  public GeoRocketClient(String host, int port, Vertx vertx) {
    this(vertx.createHttpClient(new HttpClientOptions()
        .setDefaultHost(host)
        .setDefaultPort(port)));
  }

  /**
   * Create a client connecting to GeoRocket running with
   * specified options
   * @param options options for HTTP client
   * @param vertx a Vert.x instance used to create a HTTP client
   */
  public GeoRocketClient(HttpClientOptions options, Vertx vertx) {
    this(vertx.createHttpClient(options));
  }
  
  /**
   * Create a client connecting to GeoRocket through the given
   * HTTP client
   * @param client the HTTP client
   */
  public GeoRocketClient(HttpClient client) {
    this.client = client;
  }
  
  /**
   * Close this client and release all resources
   */
  @Override
  public void close() {
    client.close();
  }
  
  /**
   * Create a new client accessing the GeoRocket data store
   * @return the store client
   */
  public StoreClient getStore() {
    return new StoreClient(client);
  }
}
