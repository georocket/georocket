package io.georocket.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A client connecting to the GeoRocket data store
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class StoreClient extends AbstractClient {  
  /**
   * Construct a new store client using the given HTTP client
   * @param client the HTTP client
   */
  public StoreClient(HttpClient client) {
    super(client);
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
    return prepareImport(layer, tags, null, null);
  }

  /**
   * Prepare an import. Generate an import path for the given layer and tags.
   * @param layer the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   * @param tags a collection of tags to attach to the imported data (may be
   * <code>null</code> if no tags need to be attached)
   * @param properties a collection of properties to attach to the imported
   * data (may be <code>null</code> if no properties need to be attached)
   * @return the import path
   * @since 1.1.0
   */
  protected String prepareImport(String layer, Collection<String> tags,
      Collection<String> properties) {
    return prepareImport(layer, tags, properties, null);
  }

  /**
   * Prepare an import. Generate an import path for the given layer and tags.
   * @param layer the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   * @param tags a collection of tags to attach to the imported data (may be
   * <code>null</code> if no tags need to be attached)
   * @param properties a collection of properties to attach to the imported
   * data (may be <code>null</code> if no properties need to be attached)
   * @param fallbackCRS the CRS which should be used if the imported file does
   * not specify one (may be <code>null</code>)
   * @return the import path
   * @since 1.1.0
   */
  protected String prepareImport(String layer, Collection<String> tags,
      Collection<String> properties, String fallbackCRS) {
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

    boolean hasTags = tags != null && !tags.isEmpty();
    if (hasTags) {
      path += "?tags=" + urlencode(String.join(",", tags));
    }

    boolean hasProperties = properties != null && !properties.isEmpty();
    if (hasProperties) {
      if (hasTags) {
        path += "&props=";
      } else {
        path += "?props=";
      }
      path += urlencode(String.join(",", properties));
    }

    if (fallbackCRS != null && !fallbackCRS.isEmpty()) {
      if (hasTags || hasProperties) {
        path += "&fallbackCRS=";
      } else {
        path += "?fallbackCRS=";
      }
      path += urlencode(String.join(",", fallbackCRS));
    }

    return path;
  }

  /**
   * Converts a handler with a result to one without a result
   * @param handler the handler to convert
   * @param <T> the type of the result
   * @return the converted handler
   */
  private <T> Handler<AsyncResult<T>> ignoreResult(Handler<AsyncResult<Void>> handler) {
    return ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handler.handle(Future.succeededFuture());
      }
    };
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
    return startImport(new ImportParams(), ignoreResult(handler));
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
    return startImport(new ImportParams().setLayer(layer), ignoreResult(handler));
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
   * @deprecated Use {@link #startImport(ImportParams, Handler)} instead
   */
  @Deprecated
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Handler<AsyncResult<Void>> handler) {
    return startImport(new ImportParams()
        .setLayer(layer)
        .setTags(tags),
      ignoreResult(handler));
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
   * @param properties a collection of properties to attach to the imported
   * data (may be <code>null</code> if no properties need to be attached)
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   * @since 1.1.0
   * @deprecated Use {@link #startImport(ImportParams, Handler)} instead
   */
  @Deprecated
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Collection<String> properties, Handler<AsyncResult<Void>> handler) {
    return startImport(new ImportParams()
        .setLayer(layer)
        .setTags(tags)
        .setProperties(properties),
      ignoreResult(handler));
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
   * @deprecated Use {@link #startImport(ImportParams, Handler)} instead
   */
  @Deprecated
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      long size, Handler<AsyncResult<Void>> handler) {
    return startImport(new ImportParams()
        .setLayer(layer)
        .setTags(tags)
        .setSize(size),
      ignoreResult(handler));
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
   * @param properties a collection of properties to attach to the imported
   * data (may be <code>null</code> if no properties need to be attached)
   * @param size size of the data to be sent in bytes
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   * @since 1.1.0
   * @deprecated Use {@link #startImport(ImportParams, Handler)} instead
   */
  @Deprecated
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Collection<String> properties, long size, Handler<AsyncResult<Void>> handler) {
    return startImport(new ImportParams()
        .setLayer(layer)
        .setTags(tags)
        .setProperties(properties)
        .setSize(size),
      ignoreResult(handler));
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
   * @deprecated Use {@link #startImport(ImportParams, Handler)} instead
   */
  @Deprecated
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Optional<Long> size, Handler<AsyncResult<Void>> handler) {
    return startImport(new ImportParams()
        .setLayer(layer)
        .setTags(tags)
        .setSize(size.orElse(null)),
      ignoreResult(handler));
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
   * @param properties a collection of properties to attach to the imported
   * data (may be <code>null</code> if no properties need to be attached)
   * @param size size of the data to be sent in bytes (optional)
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   * @since 1.1.0
   * @deprecated Use {@link #startImport(ImportParams, Handler)} instead
   */
  @Deprecated
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Collection<String> properties, Optional<Long> size, Handler<AsyncResult<Void>> handler) {
    return startImport(new ImportParams()
        .setLayer(layer)
        .setTags(tags)
        .setProperties(properties)
        .setSize(size.orElse(null)),
      ignoreResult(handler));
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
   * @param properties a collection of properties to attach to the imported
   * data (may be <code>null</code> if no properties need to be attached)
   * @param size size of the data to be sent in bytes (optional)
   * @param fallbackCRS the CRS which should be used if the imported file does
   * not specify one (may be <code>null</code>)
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   * @since 1.1.0
   * @deprecated Use {@link #startImport(ImportParams, Handler)} instead
   */
  @Deprecated
  public WriteStream<Buffer> startImport(String layer, Collection<String> tags,
      Collection<String> properties, Optional<Long> size,
      String fallbackCRS, Handler<AsyncResult<Void>> handler) {
    return startImport(new ImportParams()
        .setLayer(layer)
        .setTags(tags)
        .setProperties(properties)
        .setSize(size.orElse(null))
        .setFallbackCRS(fallbackCRS),
      ignoreResult(handler));
  }

  /**
   * <p>Start importing data to GeoRocket. The method opens a connection to the
   * GeoRocket server and returns a {@link WriteStream} that can be used to
   * send data.</p>
   * <p>The caller is responsible for closing the stream (and ending
   * the import process) through {@link WriteStream#end()} and handling
   * exceptions through {@link WriteStream#exceptionHandler(Handler)}.</p>
   * @param params import parameters
   * @param handler a handler that will be called when the data has been
   * imported by the GeoRocket server
   * @return a {@link WriteStream} that can be used to send data
   * @since 1.3.0
   */
  public WriteStream<Buffer> startImport(ImportParams params,
      Handler<AsyncResult<ImportResult>> handler) {
    if (params == null) {
      params = new ImportParams();
    }
    String path = prepareImport(params.getLayer(), params.getTags(),
        params.getProperties(), params.getFallbackCRS());
    HttpClientRequest request = client.post(path);

    if (params.getSize() != null) {
      request.putHeader("Content-Length", params.getSize().toString());
    } else {
      // content length is not set, therefore chunked encoding must be set
      request.setChunked(true);
    }

    if (params.getCompression() == ImportParams.Compression.GZIP) {
      request.putHeader("Content-Encoding", "gzip");
    }

    request.handler(response -> {
      if (response.statusCode() != 202) {
        fail(response, handler, message -> {
          ClientAPIException e = ClientAPIException.parse(message);

          String msg = String.format(
            "GeoRocket did not accept the file (status code %s: %s) %s",
            response.statusCode(),
            response.statusMessage(),
            e.getMessage());

          return new ClientAPIException(e.getType(), msg);
        });
      } else {
        handler.handle(Future.succeededFuture(new ImportResult()));
      }
    });

    return configureRequest(request);
  }

  /**
   * <p>Export the contents of the whole GeoRocket data store. Return a
   * {@link ReadStream} from which merged chunks can be read.</p>
   * <p>The caller is responsible for handling exceptions through
   * {@link ReadStream#exceptionHandler(Handler)}.</p>
   * @param handler a handler that will receive the {@link ReadStream}
   */
  public void search(Handler<AsyncResult<ReadStream<Buffer>>> handler) {
    search(new SearchParams(), ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handler.handle(Future.succeededFuture(ar.result().getResponse()));
      }
    });
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
    search(new SearchParams().setQuery(query), ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handler.handle(Future.succeededFuture(ar.result().getResponse()));
      }
    });
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
   * @deprecated Use {@link #search(SearchParams, Handler)} instead
   */
  @Deprecated
  public void search(String query, String layer,
      Handler<AsyncResult<ReadStream<Buffer>>> handler) {
    search(new SearchParams().setQuery(query).setLayer(layer), ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handler.handle(Future.succeededFuture(ar.result().getResponse()));
      }
    });
  }

  /**
   * <p>Search the GeoRocket data store and return a {@link SearchResult}
   * containing a {@link ReadStream} of merged chunks matching the given options.</p>
   * <p>If {@link SearchParams#getQuery()} returns {@code null} or an empty
   * String, all chunks from the the layer returned by {@link SearchParams#getLayer()}
   * (and all sub-layers) will be returned. If the layer is also {@code null}
   * or empty, the contents of the whole data store will be returned.</p>
   * <p>The caller is responsible for handling exceptions through
   * {@link ReadStream#exceptionHandler(Handler)}.</p>
   * @param params search parameters
   * @param handler a handler that will receive the {@link SearchResult} object
   */
  public void search(SearchParams params, Handler<AsyncResult<SearchResult>> handler) {
    if (params == null) {
      params = new SearchParams();
    }
    String query = params.getQuery();
    String layer = params.getLayer();
    if ((query == null || query.isEmpty()) && (layer == null || layer.isEmpty())) {
      handler.handle(Future.failedFuture("No search query and no layer given. "
          + "Do you really wish to export/query the whole data store? If so, "
          + "set the layer to '/'."));
      return;
    }
    String queryPath = prepareQuery(query, layer, params.isOptimisticMerging());
    HttpClientRequest request = client.get(getEndpoint() + queryPath);
    request.putHeader("TE", "trailers");
    request.exceptionHandler(t -> handler.handle(Future.failedFuture(t)));
    request.handler(response -> {
      if (response.statusCode() == 404) {
        fail(response, handler, message -> new NoSuchElementException(
            ClientAPIException.parse(message).getMessage()));
      } else if (response.statusCode() != 200) {
        fail(response, handler);
      } else {
        handler.handle(Future.succeededFuture(new SearchResult(
            new SearchReadStream(response))));
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
      handler.handle(Future.failedFuture("No search query and no layer given. "
          + "Do you really wish to delete the whole data store? If so, "
          + "set the layer to '/'."));
      return;
    }
    String queryPath = prepareQuery(query, layer);
    HttpClientRequest request = client.delete(getEndpoint() + queryPath);
    request.exceptionHandler(t -> {
      handler.handle(Future.failedFuture(t));
    });
    request.handler(response -> {
      if (response.statusCode() != 204) {
        fail(response, handler);
      } else {
        response.endHandler(v -> {
          handler.handle(Future.succeededFuture());
        });
      }
    });
    configureRequest(request).end();
  }

  /**
   * <p>Set properties of chunks in the GeoRocket data store. If a
   * property with the same key already exists, its value will be
   * overwritten.</p>.
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * properties will be appended to all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose
   * properties should be updated (or <code>null</code> if the
   * properties of all chunks in all sub-layers from the given
   * <code>layer</code> should be updated)
   * @param layer the absolute path to the layer in which to update
   * properties (or <code>null</code> if the entire store should be
   * queried to find the chunks, whose properties should be updated)
   * @param properties a collection of properties to set
   * @param handler a handler that will be called when the operation
   * has finished
   */
  public void setProperties(String query, String layer, List<String> properties,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.PUT, getEndpoint(), "properties", query, layer,
      properties, handler);
  }

  /**
   * <p>Remove properties from chunks in the GeoRocket data store.</p>
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * properties will be removed from all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose
   * properties should be updated (or <code>null</code> if the
   * properties of all chunks in all sub-layers from the given
   * <code>layer</code> should be updated)
   * @param layer the absolute path to the layer in which to update
   * properties (or <code>null</code> if the entire store should be
   * queried to find the chunks, whose properties should be updated)
   * @param properties a collection of properties to remove from
   * the queried chunks
   * @param handler a handler that will be called when the operation
   * has finished
   */
  public void removeProperties(String query, String layer, List<String> properties,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.DELETE, getEndpoint(), "properties", query, layer,
      properties, handler);
  }

  /**
   * <p>Append tags to chunks in the GeoRocket data store.</p>
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * tags will be appended to all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose tags should
   * be updated (or <code>null</code> if the tags of all chunks in all
   * sub-layers from the given <code>layer</code> should be updated)
   * @param layer the absolute path to the layer from which to update tags
   * (or <code>null</code> if the entire store should be queried to find
   * the chunks, whose tags should be updated)
   * @param tags a collection of tags to be removed from the queried chunks
   * @param handler a handler that will be called when the operation has
   * finished
   */
  public void appendTags(String query, String layer, List<String> tags,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.PUT, getEndpoint(), "tags", query, layer,
      tags, handler);
  }

  /**
   * <p>Remove tags from chunks in the GeoRocket data store.</p>
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * tags will be removed from all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose tags should
   * be updated (or <code>null</code> if the tags of all chunks in all
   * sub-layers from the given <code>layer</code> should be updated)
   * @param layer the absolute path to the layer from which to update tags
   * (or <code>null</code> if the entire store should be queried to find
   * the chunks, whose tags should be updated)
   * @param tags a collection of tags to be removed from the queried chunks
   * @param handler a handler that will be called when the operation has
   * finished
   */
  public void removeTags(String query, String layer, List<String> tags,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.DELETE, getEndpoint(), "tags", query, layer,
      tags, handler);
  }

  /**
   * <p>Search the GeoRocket store for all values of the specified
   * property and return a {@link ReadStream} of merged values matching the
   * given criteria.</p>
   * <p>If <code>query</code> is <code>null</code> or empty all chunks from
   * the given <code>layer</code> (and all sub-layers) will be searched for the
   * property. If <code>layer</code> is also <code>null</code> or empty the
   * contents of the whole data store will be searched for the property.</p>
   * <p>The caller is responsible for handling exceptions through
   * {@link ReadStream#exceptionHandler(Handler)}.</p>
   * @param property the name of the property
   * @param query a search query specifying which chunks should be searched
   * (may be <code>null</code>)
   * @param layer the name of the layer where to search for chunks recursively
   * (may be <code>null</code>)
   * @param handler a handler that will receive the {@link ReadStream} from
   * which the merged values matching the given criteria can be read
   */
  public void getPropertyValues(String property, String query, String layer,
    Handler<AsyncResult<ReadStream<Buffer>>> handler) {
    getWithParameter(getEndpoint(), "property", property, query, layer, handler);
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
