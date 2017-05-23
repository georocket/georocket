package io.georocket.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A client connecting to the GeoRocket api
 * @since 1.1.0
 * @author Tim Hellhake
 */
public class AbstractClient {
  /**
   * Prepare a query. Generate a query path for the given search query and layer.
   * @param query the search query (may be <code>null</code>)
   * @param layer the layer to export (may be <code>null</code>)
   * @return the query path
   */
  protected String prepareQuery(String query, String layer) {
    if (layer == null || layer.isEmpty()) {
      layer = "/";
    } else {
      layer = Stream.of(layer.split("/"))
        .map(this::urlencode)
        .collect(Collectors.joining("/"));
    }

    if (!layer.endsWith("/")) {
      layer += "/";
    }
    if (!layer.startsWith("/")) {
      layer = "/" + layer;
    }

    String urlQuery = "";
    if (query != null && !query.isEmpty()) {
      urlQuery = "?search=" + urlencode(query);
    }

    return layer + urlQuery;
  }

  /**
   * Convenience method to URL-encode a string
   * @param str the string
   * @return the encoded string
   */
  protected String urlencode(String str) {
    try {
      return URLEncoder.encode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Configure an HTTP request. The default implementation of this method does
   * nothing. Sub-classes may override if they want to configure a request
   * before it is sent.
   * @param request request the request to configure
   * @return same {@link HttpClientRequest} as given but with options set
   */
  protected HttpClientRequest configureRequest(HttpClientRequest request) {
    return request;
  }
  
  /**
   * Parses an HTTP response and calls the given handler with the
   * parsed error message
   * @param <T> the type of the handler's result
   * @param response the HTTP response
   * @param handler the handler to call
   */
  protected static <T> void fail(HttpClientResponse response,
    Handler<AsyncResult<T>> handler) {
    fail(response, handler, ClientAPIException::parse);
  }

  /**
   * Parses an HTTP response, maps it to an exception and then calls the
   * given handler
   * @param <T> the type of the handler's result
   * @param response the HTTP response
   * @param handler the handler to call
   * @param map a function that maps the parsed error message to an exception
   */
  protected static <T> void fail(HttpClientResponse response,
    Handler<AsyncResult<T>> handler, Function<String, Throwable> map) {
    response.bodyHandler(buffer ->
      handler.handle(Future.failedFuture(map.apply(buffer.toString()))));
  }
}
