package io.georocket.index.elasticsearch;

import java.util.Map;

import io.georocket.util.HttpException;
import io.georocket.util.RxUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import rx.Observable;
import rx.Scheduler;

/**
 * An Elasticsearch client using the HTTP API
 * @author Michel Kraemer
 */
public class ElasticsearchClient {
  private static Logger log = LoggerFactory.getLogger(ElasticsearchClient.class);
  
  /**
   * The index to query against
   */
  private final String index;
  
  /**
   * The Vert.x instance
   */
  private final Vertx vertx;
  
  /**
   * The HTTP client used to talk to Elasticsearch
   */
  private final HttpClient client;
  
  /**
   * Connect to an Elasticsearch instance
   * @param host the host to connect to
   * @param port the port on which Elasticsearch is listening for HTTP
   * requests (most likely 9200)
   * @param index the index to query against
   * @param vertx a Vert.x instance
   */
  public ElasticsearchClient(String host, int port, String index, Vertx vertx) {
    this.index = index;
    this.vertx = vertx;
    
    HttpClientOptions clientOptions = new HttpClientOptions()
        .setDefaultHost(host)
        .setDefaultPort(port)
        .setKeepAlive(true);
    client = vertx.createHttpClient(clientOptions);
  }
  
  /**
   * Close the client and release all resources
   */
  public void close() {
    client.close();
  }
  
  /**
   * Insert a number of documents in one bulk request
   * @param type the type of the documents to insert
   * @param documents maps document IDs to the actual documents to insert
   * @return the parsed bulk response from the server
   * @see #bulkResponseHasErrors(JsonObject)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  public Observable<JsonObject> bulkInsert(String type, Map<String, JsonObject> documents) {
    String uri = "/" + index + "/" + type + "/_bulk";
    
    // prepare the whole body now because it's much faster to send
    // it at once instead of using HTTP chunked mode.
    StringBuilder body = new StringBuilder();
    for (Map.Entry<String, JsonObject> e : documents.entrySet()) {
      String id = e.getKey();
      String source = e.getValue().encode();
      JsonObject subject = new JsonObject().put("_id", id);
      body.append("{\"index\":" + subject.encode() + "}\n" + source + "\n");
    }
    
    return performRequestRetry(HttpMethod.POST, uri, body.toString());
  }
  
  /**
   * Perform a search and start scrolling over the result documents
   * @param type the type of the documents to search
   * @param query the query to send
   * @param pageSize the number of objects to return in one response
   * @param timeout the time after which the returned scroll id becomes invalid
   * @return an object containing the search hits and a scroll id that can
   * be passed to {@link #continueScroll(String, String)} to get more results
   */
  public Observable<JsonObject> beginScroll(String type, JsonObject query,
      int pageSize, String timeout) {
    return beginScroll(type, query, null, pageSize, timeout);
  }
  
  /**
   * Perform a search and start scrolling over the result documents. You can
   * either specify a <code>query</code>, a <code>postFilter</code> or both,
   * but one of them is required.
   * @param type the type of the documents to search
   * @param query the query to send (may be <code>null</code>, in this case
   * <code>postFilter</code> must be set)
   * @param postFilter a filter to apply (may be <code>null</code>, in this case
   * <code>query</code> must be set)
   * @param pageSize the number of objects to return in one response
   * @param timeout the time after which the returned scroll id becomes invalid
   * @return an object containing the search hits and a scroll id that can
   * be passed to {@link #continueScroll(String, String)} to get more results
   */
  public Observable<JsonObject> beginScroll(String type, JsonObject query,
      JsonObject postFilter, int pageSize, String timeout) {
    return beginScroll(type, query, postFilter, null, pageSize, timeout);
  }

  /**
   * Perform a search, apply an aggregation, and start scrolling over the
   * result documents. You can either specify a <code>query</code>, a
   * <code>postFilter</code> or both, but one of them is required.
   * @param type the type of the documents to search
   * @param query the query to send (may be <code>null</code>, in this case
   * <code>postFilter</code> must be set)
   * @param postFilter a filter to apply (may be <code>null</code>, in this case
   * <code>query</code> must be set)
   * @param aggregation the aggregation to apply. Can be <code>null</code>
   * @param pageSize the number of objects to return in one response
   * @param timeout the time after which the returned scroll id becomes invalid
   * @return an object containing the search hits and a scroll id that can
   * be passed to {@link #continueScroll(String, String)} to get more results
   */
  public Observable<JsonObject> beginScroll(String type, JsonObject query,
      JsonObject postFilter, JsonObject aggregation, int pageSize, String timeout) {
    String uri = "/" + index + "/" + type + "/_search";
    uri += "?scroll=" + timeout;
    
    JsonObject source = new JsonObject();
    source.put("size", pageSize);
    if (query != null) {
      source.put("query", query);
    }
    if (postFilter != null) {
      source.put("post_filter", postFilter);
    }
    if (aggregation != null) {
      source.put("aggs", aggregation);
    }
    
    // sort by doc (fastest way to scroll)
    source.put("sort", new JsonArray().add("_doc"));
    
    return performRequestRetry(HttpMethod.GET, uri, source.encode());
  }
  
  /**
   * Continue scrolling through search results. Call
   * {@link #beginScroll(String, JsonObject, int, String)} to get a scroll id
   * @param scrollId the scroll id
   * @param timeout the time after which the scroll id becomes invalid
   * @return an object containing new search hits and possibly a new scroll id
   */
  public Observable<JsonObject> continueScroll(String scrollId, String timeout) {
    String uri = "/_search/scroll";
    
    JsonObject source = new JsonObject();
    source.put("scroll", timeout);
    source.put("scroll_id", scrollId);
    
    return performRequestRetry(HttpMethod.GET, uri, source.encode());
  }
  
  /**
   * Delete a number of documents in one bulk request
   * @param type the type of the documents to delete
   * @param ids the IDs of the documents to delete
   * @return the parsed bulk response from the server
   * @see #bulkResponseHasErrors(JsonObject)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  public Observable<JsonObject> bulkDelete(String type, JsonArray ids) {
    String uri = "/" + index + "/" + type + "/_bulk";
    
    // prepare the whole body now because it's much faster to send
    // it at once instead of using HTTP chunked mode.
    StringBuilder body = new StringBuilder();
    for (int i = 0; i < ids.size(); ++i) {
      String id = ids.getString(i);
      JsonObject subject = new JsonObject().put("_id", id);
      body.append("{\"delete\":" + subject.encode() + "}\n");
    }
    
    return performRequestRetry(HttpMethod.POST, uri, body.toString());
  }

  /**
   * Check if the index exists
   * @return an observable emitting <code>true</code> if the index exists or
   * <code>false</code> otherwise
   */
  public Observable<Boolean> indexExists() {
    return exists("/" + index);
  }

  /**
   * Check if the type of the index exists
   * @param type the type
   * @return an observable emitting <code>true</code> if the type of
   * the index exists or <code>false</code> otherwise
   */
  public Observable<Boolean> typeExists(String type) {
    return exists("/" + index + "/" + type);
  }

  /**
   * Check if the given URI exists by sending an empty request
   * @param uri uri to check
   * @return an observable emitting <code>true</code> if the request
   * was successful or <code>false</code> otherwise
   */
  private Observable<Boolean> exists(String uri) {
    return performRequestRetry(HttpMethod.HEAD, uri, null)
      .map(o -> true)
      .onErrorResumeNext(t -> {
        if (t instanceof HttpException && ((HttpException)t).getStatusCode() == 404) {
          return Observable.just(false);
        }
        return Observable.error(t);
      });
  }
  
  /**
   * Create the index
   * @return an observable emitting <code>true</code> if the index creation
   * was acknowledged by Elasticsearch, <code>false</code> otherwise
   */
  public Observable<Boolean> createIndex() {
    String uri = "/" + index;
    return performRequestRetry(HttpMethod.PUT, uri, null)
      .map(res -> res.getBoolean("acknowledged", true));
  }
  
  /**
   * Add mapping for the given type
   * @param type the type
   * @param mapping the mapping to set for the index
   * @return an observable emitting <code>true</code> if the operation
   * was acknowledged by Elasticsearch, <code>false</code> otherwise
   */
  public Observable<Boolean> putMapping(String type, JsonObject mapping) {
    String uri = "/" + index + "/_mapping/" + type;
    return performRequestRetry(HttpMethod.PUT, uri, mapping.encode())
      .map(res -> res.getBoolean("acknowledged", true));
  }
  
  /**
   * Check if Elasticsearch is running and if it answers to a simple request
   * @return <code>true</code> if Elasticsearch is running, <code>false</code>
   * otherwise
   */
  public Observable<Boolean> isRunning() {
    HttpClientRequest req = client.head("/");
    return performRequest(req, null).map(v -> true).onErrorReturn(t -> false);
  }

  /**
   * Perform an HTTP request and if it fails retry it a couple of times
   * @param method the HTTP method
   * @param uri the request URI
   * @param body the body to send in the request (may be null)
   * @return an observable emitting the parsed response body (may be null if no
   * body was received)
   */
  private Observable<JsonObject> performRequestRetry(HttpMethod method,
      String uri, String body) {
    Scheduler scheduler = RxHelper.scheduler((io.vertx.core.Vertx)vertx.getDelegate());
    return Observable.<JsonObject>create(subscriber -> {
      HttpClientRequest req = client.request(method, uri);
      performRequest(req, body).subscribe(subscriber);
    }).retryWhen(errors -> {
      Observable<Throwable> o = errors.flatMap(error -> {
        if (error instanceof HttpException) {
          // immediately forward HTTP errors, don't retry
          return Observable.error(error);
        }
        return Observable.just(error);
      });
      return RxUtils.makeRetry(5, 1000, scheduler, log).call(o);
    }, scheduler);
  }
  
  /**
   * Perform an HTTP request
   * @param req the request to perform
   * @param body the body to send in the request (may be null)
   * @return an observable emitting the parsed response body (may be null if no
   * body was received)
   */
  private Observable<JsonObject> performRequest(HttpClientRequest req,
      String body) {
    ObservableFuture<JsonObject> observable = RxHelper.observableFuture();
    Handler<AsyncResult<JsonObject>> handler = observable.toHandler();
    
    req.exceptionHandler(t -> {
      handler.handle(Future.failedFuture(t));
    });
    
    req.handler(res -> {
      int code = res.statusCode();
      if (code == 200) {
        Buffer buf = Buffer.buffer();
        res.handler(b -> {
          buf.appendBuffer(b);
        });
        res.endHandler(v -> {
          if (buf.length() > 0) {
            handler.handle(Future.succeededFuture(buf.toJsonObject()));
          } else {
            handler.handle(Future.succeededFuture());
          }
        });
      } else {
        handler.handle(Future.failedFuture(new HttpException(code)));
      }
    });
    
    if (body != null) {
      req.setChunked(false);
      req.putHeader("Content-Length", String.valueOf(body.length()));
      req.end(body);
    } else {
      req.end();
    }
    
    return observable;
  }
  
  /**
   * Check if the given bulk response contains errors
   * @param response the bulk response
   * @return <code>true</code> if the response has errors, <code>false</code>
   * otherwise
   * @see #bulkInsert(String, Map)
   * @see #bulkDelete(String, JsonArray)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  public boolean bulkResponseHasErrors(JsonObject response) {
    return response.getBoolean("errors", false);
  }
  
  /**
   * Builds an error message from a bulk response containing errors
   * @param response the response containing errors
   * @return the error message or null if the bulk response does not contain
   * errors
   * @see #bulkInsert(String, Map)
   * @see #bulkDelete(String, JsonArray)
   * @see #bulkResponseHasErrors(JsonObject)
   */
  public String bulkResponseGetErrorMessage(JsonObject response) {
    if (!bulkResponseHasErrors(response)) {
      return null;
    }
    
    StringBuilder res = new StringBuilder();
    res.append("Errors in bulk operation:");
    JsonArray items = response.getJsonArray("items", new JsonArray());
    for (Object o : items) {
      JsonObject jo = (JsonObject)o;
      for (String key : jo.fieldNames()) {
        JsonObject op = jo.getJsonObject(key);
        if (bulkResponseItemHasErrors(op)) {
          res.append(bulkResponseItemGetErrorMessage(op));
        }
      }
    }
    return res.toString();
  }
  
  /**
   * Check if an item of a bulk response has an error
   * @param item the item
   * @return <code>true</code> if the item has an error, <code>false</code>
   * otherwise
   */
  public boolean bulkResponseItemHasErrors(JsonObject item) {
    return item.getJsonObject("error") != null;
  }
  
  /**
   * Builds an error message from a bulk response item
   * @param item the item
   * @return the error message or null if the bulk response item does not
   * contain an error
   */
  public String bulkResponseItemGetErrorMessage(JsonObject item) {
    if (!bulkResponseItemHasErrors(item)) {
      return null;
    }
    
    StringBuilder res = new StringBuilder();
    JsonObject error = item.getJsonObject("error");
    if (error != null) {
      String id = item.getString("_id");
      String type = error.getString("type");
      String reason = error.getString("reason");
      res.append("\n[id: [");
      res.append(id);
      res.append("], type: [");
      res.append(type);
      res.append("], reason: [");
      res.append(reason);
      res.append("]]");
    }
    
    return res.toString();
  }
}
