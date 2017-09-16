package io.georocket.index.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.jooq.lambda.tuple.Tuple2;

import io.georocket.util.HttpException;
import io.georocket.util.RxUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.NoStackTraceThrowable;
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
import rx.Single;

/**
 * An Elasticsearch client using the HTTP API
 * @author Michel Kraemer
 */
public class RemoteElasticsearchClient implements ElasticsearchClient {
  private static Logger log = LoggerFactory.getLogger(RemoteElasticsearchClient.class);
  
  /**
   * The index to query against
   */
  private final String index;
  
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
  public RemoteElasticsearchClient(String host, int port, String index,
      Vertx vertx) {
    this.index = index;
    
    HttpClientOptions clientOptions = new HttpClientOptions()
        .setDefaultHost(host)
        .setDefaultPort(port)
        .setKeepAlive(true);
    client = vertx.createHttpClient(clientOptions);
  }

  @Override
  public void close() {
    client.close();
  }
  
  @Override
  public Single<JsonObject> bulkInsert(String type,
      List<Tuple2<String, JsonObject>> documents) {
    String uri = "/" + index + "/" + type + "/_bulk";
    
    // prepare the whole body now because it's much faster to send
    // it at once instead of using HTTP chunked mode.
    StringBuilder body = new StringBuilder();
    for (Tuple2<String, JsonObject> e : documents) {
      String id = e.v1;
      String source = e.v2.encode();
      JsonObject subject = new JsonObject().put("_id", id);
      body.append("{\"index\":" + subject.encode() + "}\n" + source + "\n");
    }
    
    return performRequestRetry(HttpMethod.POST, uri, body.toString());
  }
  
  @Override
  public Single<JsonObject> beginScroll(String type, JsonObject query,
      JsonObject postFilter, JsonObject aggregations, JsonObject parameters, String timeout) {
    String uri = "/" + index + "/" + type + "/_search";
    uri += "?scroll=" + timeout;
    
    JsonObject source = new JsonObject();
    parameters.forEach(entry ->
      source.put(entry.getKey(), entry.getValue()));

    if (query != null) {
      source.put("query", query);
    }
    if (postFilter != null) {
      source.put("post_filter", postFilter);
    }
    if (aggregations != null) {
      source.put("aggs", aggregations);
    }
    
    // sort by doc (fastest way to scroll)
    source.put("sort", new JsonArray().add("_doc"));
    
    return performRequestRetry(HttpMethod.GET, uri, source.encode());
  }
  
  @Override
  public Single<JsonObject> continueScroll(String scrollId, String timeout) {
    String uri = "/_search/scroll";
    
    JsonObject source = new JsonObject();
    source.put("scroll", timeout);
    source.put("scroll_id", scrollId);
    
    return performRequestRetry(HttpMethod.GET, uri, source.encode());
  }
  
  @Override
  public Single<JsonObject> search(String type, JsonObject query,
      JsonObject postFilter, JsonObject aggregations, JsonObject parameters) {
    String uri = "/" + index + "/" + type + "/_search";
    
    JsonObject source = new JsonObject();
    parameters.forEach(entry -> 
      source.put(entry.getKey(), entry.getValue()));
    
    if (query != null) {
      source.put("query", query);
    }
    if (postFilter != null) {
      source.put("post_filter", postFilter);
    }
    if (aggregations != null) {
      source.put("aggs", aggregations);
    }
    
    return performRequestRetry(HttpMethod.GET, uri, source.encode());
  }

  @Override
  public Single<Long> count(String type, JsonObject query) {
    String uri = "/" + index + "/" + type + "/_count";

    JsonObject source = new JsonObject();
    if (query != null) {
      source.put("query", query);
    }

    return performRequestRetry(HttpMethod.GET, uri, source.encode())
      .flatMap(sr -> {
        Long l = sr.getLong("count");
        if (l == null) {
          return Single.error(new NoStackTraceThrowable(
              "Could not count documents"));
        }
        return Single.just(l);
      });
  }

  @Override
  public Single<JsonObject> updateByQuery(String type, JsonObject postFilter,
      JsonObject script) {
    String uri = "/" + index + "/" + type + "/_update_by_query";

    JsonObject source = new JsonObject();
    if (postFilter != null) {
      source.put("post_filter", postFilter);
    }
    if (script != null) {
      source.put("script", script);
    }

    return performRequestRetry(HttpMethod.POST, uri, source.encode());
  }
  
  @Override
  public Single<JsonObject> bulkDelete(String type, JsonArray ids) {
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
  
  @Override
  public Single<Void> aliases(JsonArray actions) {
    String uri = "/_aliases";

    JsonObject body = new JsonObject()
      .put("actions", actions);

    return performRequestRetry(HttpMethod.POST, uri, body.encode())
      .map(v -> null);
  }

  @Override
  public Single<JsonObject> getAliases() {
    String uri = "/_aliases";

    return performRequestRetry(HttpMethod.GET, uri, null);
  }

  @Override
  public Single<Void> reindex(JsonObject src, JsonObject dest, JsonObject script) {
    String uri = "/_reindex";

    JsonObject body = new JsonObject()
      .put("source", src)
      .put("dest", dest);

    if (script != null) {
      body.put("script", script);
    }

    return performRequestRetry(HttpMethod.POST, uri, body.encode())
      .map(v -> null);
  }

  @Override
  public Single<Void> delete() {
    String uri = "/" + index;

    return performRequestRetry(HttpMethod.DELETE, uri, null)
      .map(v -> null);
  }

  @Override
  public Single<List<String>> indices() {
    String uri = "/_all/_settings";

    return performRequestRetry(HttpMethod.GET, uri, null)
      .map(settings -> new ArrayList<>(settings.fieldNames()));
  }

  @Override
  public Single<Boolean> indexExists() {
    return exists("/" + index);
  }
  
  @Override
  public Single<Boolean> typeExists(String type) {
    return exists("/" + index + "/" + type);
  }
  
  /**
   * Check if the given URI exists by sending an empty request
   * @param uri uri to check
   * @return an observable emitting <code>true</code> if the request
   * was successful or <code>false</code> otherwise
   */
  private Single<Boolean> exists(String uri) {
    return performRequestRetry(HttpMethod.HEAD, uri, null)
      .map(o -> true)
      .onErrorResumeNext(t -> {
        if (t instanceof HttpException && ((HttpException)t).getStatusCode() == 404) {
          return Single.just(false);
        }
        return Single.error(t);
      });
  }
  
  @Override
  public Single<Boolean> createIndex() {
    return createIndex(null);
  }

  @Override
  public Single<Boolean> createIndex(JsonObject settings) {
    String uri = "/" + index;

    String body = settings == null ? null :
      new JsonObject().put("settings", settings).encode();

    return performRequestRetry(HttpMethod.PUT, uri, body)
        .map(res -> res.getBoolean("acknowledged", true));
  }

  /**
   * Ensure the Elasticsearch index exists
   * @return an observable that will emit a single item when the index has
   * been created or if it already exists
   */
  @Override
  public Single<Void> ensureIndex() {
    // check if the index exists
    return indexExists().flatMap(exists -> {
      if (exists) {
        return Single.just(null);
      } else {
        // index does not exist. create it.
        return createIndex().flatMap(ack -> {
          if (ack) {
            return Single.just(null);
          }
          return Single.error(new NoStackTraceThrowable("Index creation "
            + "was not acknowledged by Elasticsearch"));
        });
      }
    });
  }
  
  @Override
  public Single<Boolean> putMapping(String type, JsonObject mapping) {
    String uri = "/" + index + "/_mapping/" + type;
    return performRequestRetry(HttpMethod.PUT, uri, mapping.encode())
      .map(res -> res.getBoolean("acknowledged", true));
  }
  
  /**
   * Ensure the Elasticsearch mapping exists
   * @param type the target type for the mapping
   * @return an observable that will emit a single item when the mapping has
   * been created or if it already exists
   */
  @Override
  public Single<Void> ensureMapping(String type, JsonObject mapping) {
    // check if the target type exists
    return typeExists(type).flatMap(exists -> {
      if (exists) {
        return Single.just(null);
      } else {
        // target type does not exist. create the mapping.
        return putMapping(type, mapping).flatMap(ack -> {
          if (ack) {
            return Single.just(null);
          }
          return Single.error(new NoStackTraceThrowable("Mapping creation "
            + "was not acknowledged by Elasticsearch"));
        });
      }
    });
  }

  @Override
  public Single<JsonObject> getMapping(String type) {
    return getMapping(type, null);
  }

  @Override
  public Single<JsonObject> getMapping(String type, String field) {
    String uri = "/" + index + "/_mapping/" + type;
    if (field != null) {
      uri += "/field/" + field;
    }
    return performRequestRetry(HttpMethod.GET, uri, "");
  }

  @Override
  public Single<Boolean> isRunning() {
    HttpClientRequest req = client.head("/");
    return performRequest(req, null).map(v -> true).onErrorReturn(t -> false)
      .toSingle();
  }
  
  /**
   * Perform an HTTP request and if it fails retry it a couple of times
   * @param method the HTTP method
   * @param uri the request URI
   * @param body the body to send in the request (may be null)
   * @return a single emitting the parsed response body (may be null if no
   * body was received)
   */
  private Single<JsonObject> performRequestRetry(HttpMethod method,
      String uri, String body) {
    return Observable.<JsonObject>unsafeCreate(subscriber -> {
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
      return RxUtils.makeRetry(5, 1000, log).call(o);
    }).toSingle();
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
        Buffer buf = Buffer.buffer();
        res.handler(buf::appendBuffer);
        res.endHandler(v -> {
          handler.handle(Future.failedFuture(new HttpException(code, buf.toString())));
        });
      }
    });
    
    if (body != null) {
      req.setChunked(false);
      Buffer buf = Buffer.buffer(body);
      req.putHeader("Content-Length", String.valueOf(buf.length()));
      req.end(buf);
    } else {
      req.end();
    }
    
    return observable;
  }
}
