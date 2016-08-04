package io.georocket.index;

import java.util.Map;

import javax.xml.ws.http.HTTPException;

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
   * The type of objects stored in the index
   */
  private final String type;
  
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
   * @param type the type of objects stored in the index
   * @param vertx a Vert.x instance
   */
  public ElasticsearchClient(String host, int port, String index, String type,
      Vertx vertx) {
    this.index = index;
    this.type = type;
    
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
   * @param documents maps document IDs to the actual documents to insert
   * @return the parsed bulk response from the server
   * @see #bulkResponseHasErrors(JsonObject)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  public Observable<JsonObject> bulkInsert(Map<String, JsonObject> documents) {
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
   * @param query the query to send
   * @param pageSize the number of objects to return in one response
   * @param timeout the time after which the returned scroll id becomes invalid
   * @return an object containing the search hits and a scroll id that can
   * be passed to {@link #continueScroll(String, String)} to get more results
   */
  public Observable<JsonObject> beginScroll(JsonObject query, int pageSize,
      String timeout) {
    String uri = "/" + index + "/" + type + "/_search";
    uri += "?scroll=" + timeout;
    
    JsonObject source = new JsonObject();
    source.put("size", pageSize);
    source.put("query", query);
    
    // sort by doc (fastest way to scroll)
    source.put("sort", new JsonArray().add("_doc"));
    
    return performRequestRetry(HttpMethod.GET, uri, source.encode());
  }
  
  /**
   * Continue scrolling through search results. Call
   * {@link #beginScroll(JsonObject, int, String)} to get a scroll id
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
   * @param ids the IDs of the documents to delete
   * @return the parsed bulk response from the server
   * @see #bulkResponseHasErrors(JsonObject)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  public Observable<JsonObject> bulkDelete(JsonArray ids) {
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
    String uri = "/" + index;
    return performRequestRetry(HttpMethod.HEAD, uri, null)
        .map(o -> true)
        .onErrorResumeNext(t -> {
          if (t instanceof HTTPException && ((HTTPException)t).getStatusCode() == 404) {
            return Observable.just(false);
          }
          return Observable.error(t);
        });
  }
  
  /**
   * Create the index
   * @param mappings the mappings to set for the index
   * @return an observable emitting <code>true</code> if the index creation
   * was ackowledged by Elasticsearch, <code>false</code> otherwise
   */
  public Observable<Boolean> createIndex(JsonObject mappings) {
    String uri = "/" + index;
    
    JsonObject source = new JsonObject()
        .put("mappings", new JsonObject()
            .put(type, mappings));
    
    return performRequestRetry(HttpMethod.PUT, uri, source.encode()).map(res -> {
      return res.getBoolean("acknowledged", true);
    });
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
    return Observable.<JsonObject>create(subscriber -> {
      HttpClientRequest req = client.request(method, uri);
      performRequest(req, body).subscribe(subscriber);
    }).retryWhen(errors -> {
      Observable<Throwable> o = errors.flatMap(error -> {
        if (error instanceof HTTPException) {
          // immediately forward HTTP errors, don't retry
          return Observable.error(error);
        }
        return Observable.just(error);
      });
      return RxUtils.makeRetry(5, 1000, log).call(o);
    });
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
        handler.handle(Future.failedFuture(new HTTPException(code)));
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
   * @see #bulkInsert(Map)
   * @see #bulkDelete(JsonArray)
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
   * @see #bulkInsert(Map)
   * @see #bulkDelete(JsonArray)
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
