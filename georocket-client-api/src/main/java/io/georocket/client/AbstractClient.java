package io.georocket.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
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
   * HTTP client used to connect to GeoRocket
   */
  protected final HttpClient client;

  /**
   * Construct a new client using the given HTTP client
   * @param client the HTTP client
   */
  public AbstractClient(HttpClient client) {
    this.client = client;
  }

  /**
   * <p>Update a field in the chunk metadata (such as properties or tags).</p>
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, all chunks in the
   * data store will be updated.</p>
   * @param method the http method to use for the update
   * @param fieldEndpoint the path of the endpoint which should be used
   * @param fieldName the name of the field to update
   * @param query a search query specifying the chunks, whose
   * fields should be updated (or <code>null</code> if the
   * fields of all chunks in all sub-layers from the given
   * <code>layer</code> should be updated)
   * @param layer the absolute path to the layer from which to update
   * fields (or <code>null</code> if the entire store should be
   * queried to find the chunks, whose fields should be updated)
   * @param updates a collection of values to update
   * @param handler a handler that will be called when the operation
   * has finished
   * @since 1.1.0
   */
  protected void update(HttpMethod method, String fieldEndpoint, String fieldName,
    String query, String layer, List<String> updates, Handler<AsyncResult<Void>> handler) {
    if ((query == null || query.isEmpty()) && (layer == null || layer.isEmpty())) {
      handler.handle(Future.failedFuture("No search query and no layer given. "
        + "Do you really wish to update all chunks in the GeoRocket "
        + "data store? If so, set the root layer /."));
      return;
    }

    String queryPath = prepareQuery(query, layer);
    if (query == null || query.isEmpty()) {
      queryPath += "?";
    } else {
      queryPath += "&";
    }
    String values = updates.stream()
      .map(this::urlencode)
      .collect(Collectors.joining(","));

    String path = fieldEndpoint + queryPath + fieldName + "=" + values;
    HttpClientRequest request = client.request(method, path);

    request.exceptionHandler(t -> handler.handle(Future.failedFuture(t)));
    request.handler(response -> {
      if (response.statusCode() != 204) {
        fail(response, handler);
      } else {
        response.endHandler(v -> handler.handle(Future.succeededFuture()));
      }
    });
    configureRequest(request).end();
  }
  
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
