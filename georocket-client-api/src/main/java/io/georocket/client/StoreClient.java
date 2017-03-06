package io.georocket.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * A client connecting to the GeoRocket data store
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class StoreClient {
  /**
   * HTTP client used to connect to GeoRocket
   */
  private final HttpClient client;
  
  /**
   * Construct a new store client using the given HTTP client
   * @param client the HTTP client
   */
  public StoreClient(HttpClient client) {
    this.client = client;
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
   * Prepare an import. Generate an import path for the given layer and tags.
   * @param layer the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   * @param tags a collection of tags to attach to the imported data (may be
   * <code>null</code> if no tags need to be attached)
   * @return the import path
   */
  protected String prepareImport(String layer, Collection<String> tags) {
    String path = getEndpoint();

    if (layer != null && !layer.isEmpty()) {
      layer = Stream.of(layer.split("/"))
        .map(this::urlencode)
        .collect(Collectors.joining("/"));

      if (!layer.endsWith("/")) {
        layer += "/";
      }
      if (!layer.startsWith("/")) {
        layer = "/" + layer;
      }

      path += layer;
    }

    if (tags != null && !tags.isEmpty()) {
      path += "?tags=" + urlencode(String.join(",", tags));
    }

    return path;
  }

  /**
   * <p>Start importing data to GeoRocket. The method opens a connection to the
   * GeoRocket server and returns a {@link WriteStream} that can be used to
   * send data.</p>
   * <p>The caller is responsible for closing the stream (and ending
   * the import process) through {@link WriteStream#end()} and handling
   * exceptions through {@link WriteStream#exceptionHandler(Handler)}.</p>
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   */
  public WriteStream<Buffer> startImport(Handler<AsyncResult<Void>> handler) {
    return startImport(null, null, Optional.empty(), handler);
  }
  
  /**
   * <p>Start importing data to GeoRocket. The method opens a connection to the
   * GeoRocket server and returns a {@link WriteStream} that can be used to
   * send data.</p>
   * <p>The caller is responsible for closing the stream (and ending
   * the import process) through {@link WriteStream#end()} and handling
   * exceptions through {@link WriteStream#exceptionHandler(Handler)}.</p>
   * @param layer the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   */
  public WriteStream<Buffer> startImport(String layer,
      Handler<AsyncResult<Void>> handler) {
    return startImport(layer, null, Optional.empty(), handler);
  }
  
  /**
   * <p>Start importing data to GeoRocket. The method opens a connection to the
   * GeoRocket server and returns a {@link WriteStream} that can be used to
   * send data.</p>
   * <p>The caller is responsible for closing the stream (and ending
   * the import process) through {@link WriteStream#end()} and handling
   * exceptions through {@link WriteStream#exceptionHandler(Handler)}.</p>
   * @param layer the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   * @param tags a collection of tags to attach to the imported data (may be
   * <code>null</code> if no tags need to be attached)
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   */
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Handler<AsyncResult<Void>> handler) {
    return startImport(layer, tags, Optional.empty(), handler);
  }
  
  /**
   * <p>Start importing data to GeoRocket. The method opens a connection to the
   * GeoRocket server and returns a {@link WriteStream} that can be used to
   * send data.</p>
   * <p>The caller is responsible for closing the stream (and ending
   * the import process) through {@link WriteStream#end()} and handling
   * exceptions through {@link WriteStream#exceptionHandler(Handler)}.</p>
   * @param layer the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   * @param tags a collection of tags to attach to the imported data (may be
   * <code>null</code> if no tags need to be attached)
   * @param size size of the data to be sent in bytes
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   */
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      long size, Handler<AsyncResult<Void>> handler) {
    return startImport(layer, tags, Optional.of(size), handler);
  }
  
  /**
   * <p>Start importing data to GeoRocket. The method opens a connection to the
   * GeoRocket server and returns a {@link WriteStream} that can be used to
   * send data.</p>
   * <p>The caller is responsible for closing the stream (and ending
   * the import process) through {@link WriteStream#end()} and handling
   * exceptions through {@link WriteStream#exceptionHandler(Handler)}.</p>
   * @param layer the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   * @param tags a collection of tags to attach to the imported data (may be
   * <code>null</code> if no tags need to be attached)
   * @param size size of the data to be sent in bytes (optional)
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   */
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Optional<Long> size, Handler<AsyncResult<Void>> handler) {
    String path = prepareImport(layer, tags);
    HttpClientRequest request = client.post(path);

    if (size.isPresent() && size.get() != null) {
      request.putHeader("Content-Length", size.get().toString());
    } else {
      // content length is not set, therefore chunked encoding must be set
      request.setChunked(true);
    }

    request.handler(response -> {
      if (response.statusCode() != 202) {
        handler.handle(Future.failedFuture("GeoRocket did not accept the file "
            + "(status code " + response.statusCode() + ": " + response.statusMessage() + ")"));
      } else {
        handler.handle(Future.succeededFuture());
      }
    });

    return configureRequest(request);
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
   * <p>Export the contents of the whole GeoRocket data store. Return a
   * {@link ReadStream} from which merged chunks can be read.</p>
   * <p>The caller is responsible for handling exceptions through
   * {@link ReadStream#exceptionHandler(Handler)}.</p>
   * @param handler a handler that will receive the {@link ReadStream}
   */
  public void search(Handler<AsyncResult<ReadStream<Buffer>>> handler) {
    search(null, null, handler);
  }
  
  /**
   * <p>Search the GeoRocket data store and return a {@link ReadStream} of
   * merged chunks matching the given criteria.</p>
   * <p>If <code>query</code> is <code>null</code> the contents of the
   * whole data store will be returned.</p>
   * <p>The caller is responsible for handling exceptions through
   * {@link ReadStream#exceptionHandler(Handler)}.</p>
   * @param query a search query specifying which chunks to return (may be
   * <code>null</code>)
   * @param handler a handler that will receive the {@link ReadStream} from
   * which the merged chunks matching the given criteria can be read
   */
  public void search(String query, Handler<AsyncResult<ReadStream<Buffer>>> handler) {
    search(query, null, handler);
  }
  
  /**
   * <p>Search the GeoRocket data store and return a {@link ReadStream} of
   * merged chunks matching the given criteria.</p>
   * <p>If <code>query</code> is <code>null</code> or empty all chunks from
   * the given <code>layer</code> (and all sub-layers) will be returned. If
   * <code>layer</code> is also <code>null</code> or empty the contents of the
   * whole data store will be returned.</p>
   * <p>The caller is responsible for handling exceptions through
   * {@link ReadStream#exceptionHandler(Handler)}.</p>
   * @param query a search query specifying which chunks to return (may be
   * <code>null</code>)
   * @param layer the name of the layer where to search for chunks recursively
   * (may be <code>null</code>)
   * @param handler a handler that will receive the {@link ReadStream} from
   * which the merged chunks matching the given criteria can be read
   */
  public void search(String query, String layer,
      Handler<AsyncResult<ReadStream<Buffer>>> handler) {
    String queryPath = prepareQuery(query, layer);
    HttpClientRequest request = client.get(getEndpoint() + queryPath);
    request.exceptionHandler(t -> handler.handle(Future.failedFuture(t)));
    request.handler(response -> {
      if (response.statusCode() == 404) {
        handler.handle(Future.failedFuture(new NoSuchElementException(
            response.statusMessage())));
      } else if (response.statusCode() != 200) {
        handler.handle(Future.failedFuture(response.statusMessage()));
      } else {
        handler.handle(Future.succeededFuture(response));
      }
    });
    configureRequest(request).end();
  }
  
  /**
   * <p>Delete chunks from the GeoRocket data store.</p>
   * <p>Either <code>query</code> or <code>layer</code> must be given to
   * protect against requests that accidentally delete the whole data store.
   * If you want to do so, provide an empty <code>query</code> and the root
   * layer <code>/</code>.</p>
   * @param query a search query specifying the chunks to delete (must not
   * be <code>null</code> to protect against requests that accidentally delete
   * the whole data store. If you want to do so, use
   * {@link #delete(String, String, Handler)} and provide an empty
   * <code>query</code> and the root layer <code>/</code>.)
   * @param handler a handler that will be called when the operation has
   * finished
   */
  public void delete(String query, Handler<AsyncResult<Void>> handler) {
    delete(query, null, handler);
  }
  
  /**
   * <p>Delete chunks or layers from the GeoRocket data store.</p>
   * <p>Either <code>query</code> or <code>layer</code> must be given to
   * protect against requests that accidentally delete the whole data store.
   * If you want to do so, provide an empty <code>query</code> and the root
   * layer <code>/</code>.</p>
   * @param query a search query specifying the chunks to delete (or
   * <code>null</code> if all chunks and all sub-layers from the given
   * <code>layer</code> should be deleted)
   * @param layer the absolute path to the layer from which to delete (or
   * <code>null</code> if the whole store should be searched for chunks
   * to delete)
   * @param handler a handler that will be called when the operation has
   * finished
   */
  public void delete(String query, String layer, Handler<AsyncResult<Void>> handler) {
    if ((query == null || query.isEmpty()) && (layer == null || layer.isEmpty())) {
      handler.handle(Future.failedFuture(new IllegalArgumentException(
          "No search query and no layer given. "
          + "Do you really wish to delete the whole data store? If so, "
          + "provide an empty query and the root layer /.")));
    }
    String queryPath = prepareQuery(query, layer);
    HttpClientRequest request = client.delete(getEndpoint() + queryPath);
    request.exceptionHandler(t -> {
      handler.handle(Future.failedFuture(t));
    });
    request.handler(response -> {
      if (response.statusCode() != 204) {
        handler.handle(Future.failedFuture(response.statusMessage()));
      } else {
        response.endHandler(v -> {
          handler.handle(Future.succeededFuture());
        });
      }
    });
    configureRequest(request).end();
  }

  /**
   * Return the HTTP endpoint, the GeoRocket data store path at
   * server side.
   * @return the endpoint
   */
  protected String getEndpoint() {
    return "/store";
  }
}
