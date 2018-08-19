package io.georocket.index.elasticsearch;

import io.georocket.util.HttpException;
import io.georocket.util.RxUtils;
import io.georocket.util.io.GzipWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;
import rx.Single;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An HTTP client that can perform requests against multiple hosts using
 * round robin
 * @author Michel Kraemer
 */
public class LoadBalancingHttpClient {
  private static Logger log = LoggerFactory.getLogger(LoadBalancingHttpClient.class);

  /**
   * The minimum number of bytes a body must have for it to be compressed with
   * GZIP when posting to the server. The value is smaller than the default
   * MTU (1500 bytes) to reduce compression overhead for very small bodies that
   * fit into one TCP packet.
   * @see #compressRequestBodies
   */
  private static final int MIN_COMPRESSED_BODY_SIZE = 1400;

  private final Vertx vertx;
  private final boolean compressRequestBodies;
  private int currentHost = -1;
  private List<URI> hosts = new ArrayList<>();
  private final Map<URI, HttpClient> hostsToClients = new HashMap<>();

  private HttpClientOptions defaultOptions = new HttpClientOptions()
      .setKeepAlive(true)
      .setTryUseCompression(true);

  /**
   * Constructs a new load-balancing HTTP client
   * @param vertx the current Vert.x instance
   */
  public LoadBalancingHttpClient(Vertx vertx) {
    this(vertx, false);
  }

  /**
   * Constructs a new load-balancing HTTP client
   * @param vertx the current Vert.x instance
   * @param compressRequestBodies {@code true} if bodies of HTTP requests
   * should be compressed with GZIP
   */
  public LoadBalancingHttpClient(Vertx vertx, boolean compressRequestBodies) {
    this.vertx = vertx;
    this.compressRequestBodies = compressRequestBodies;
  }


  /**
   * Set the hosts to communicate with
   * @param hosts the hosts
   */
  public void setHosts(List<URI> hosts) {
    Set<URI> uniqueHosts = new LinkedHashSet<>(hosts);

    this.hosts = new ArrayList<>(uniqueHosts);
    if (currentHost >= 0) {
      currentHost = currentHost % this.hosts.size();
    }

    // close clients of all removed hosts
    Iterator<Map.Entry<URI, HttpClient>> i = hostsToClients.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<URI, HttpClient> e = i.next();
      if (!uniqueHosts.contains(e.getKey())) {
        e.getValue().close();
        i.remove();
      }
    }

    // add clients for all new hosts
    for (URI u : uniqueHosts) {
      if (!hostsToClients.containsKey(u)) {
        hostsToClients.put(u, createClient(u));
      }
    }
  }

  /**
   * Get a copy of the list of hosts to communicate with
   * @return a copy of the list of hosts
   */
  public List<URI> getHosts() {
    return new ArrayList<>(hosts);
  }

  /**
   * Set default HTTP client options. Must be called before {@link #setHosts(List)}.
   * @param options the options
   */
  public void setDefaultOptions(HttpClientOptions options) {
    defaultOptions = options;
  }

  /**
   * Create an HttpClient for the given host
   * @param u the host
   * @return the HttpClient
   */
  private HttpClient createClient(URI u) {
    HttpClientOptions clientOptions = new HttpClientOptions(defaultOptions)
      .setDefaultHost(u.getHost())
      .setDefaultPort(u.getPort());
    return vertx.createHttpClient(clientOptions);
  }

  /**
   * Get the next available HTTP client
   * @return the client
   */
  private HttpClient nextClient() {
    currentHost = (currentHost + 1) % hosts.size();
    URI u = hosts.get(currentHost);
    return hostsToClients.get(u);
  }

  /**
   * Perform an HTTP request and convert the response to a JSON object
   * @param req the request to perform
   * @param body the body to send in the request (may be {@code null})
   * @return an observable emitting the parsed response body (may be
   * {@code null} if no body was received)
   */
  private Single<JsonObject> performRequest(HttpClientRequest req, Buffer body) {
    ObservableFuture<JsonObject> observable = RxHelper.observableFuture();
    Handler<AsyncResult<JsonObject>> handler = observable.toHandler();

    req.exceptionHandler(t -> handler.handle(Future.failedFuture(t)));

    req.handler(res -> {
      int code = res.statusCode();
      if (code == 200) {
        Buffer buf = Buffer.buffer();
        res.handler(buf::appendBuffer);
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
        res.endHandler(v -> handler.handle(Future.failedFuture(
            new HttpException(code, buf.toString()))));
      }
    });

    if (body != null) {
      req.putHeader("Accept", "application/json");
      req.putHeader("Content-Type", "application/json");
      if (compressRequestBodies && body.length() >= MIN_COMPRESSED_BODY_SIZE) {
        req.setChunked(true);
        req.putHeader("Content-Encoding", "gzip");
        GzipWriteStream gws = new GzipWriteStream(req);
        gws.end(body);
      } else {
        req.setChunked(false);
        req.putHeader("Content-Length", String.valueOf(body.length()));
        req.end(body);
      }
    } else {
      req.end();
    }

    return observable.toSingle();
  }

  /**
   * Perform an HTTP request and convert the response to a JSON object
   * @param uri the request URI
   * @return a single emitting the parsed response body (may be
   * {@code null} if no body was received)
   */
  public Single<JsonObject> performRequest(String uri) {
    return performRequest(HttpMethod.GET, uri);
  }

  /**
   * Perform an HTTP request and convert the response to a JSON object
   * @param method the HTTP method
   * @param uri the request URI
   * @return a single emitting the parsed response body (may be
   * {@code null} if no body was received)
   */
  public Single<JsonObject> performRequest(HttpMethod method, String uri) {
    return performRequest(method, uri, null);
  }

  /**
   * Perform an HTTP request and convert the response to a JSON object
   * @param method the HTTP method
   * @param uri the request URI
   * @param body the body to send in the request (may be {@code null})
   * @return a single emitting the parsed response body (may be
   * {@code null} if no body was received)
   */
  public Single<JsonObject> performRequest(HttpMethod method, String uri, Buffer body) {
    return performRequestNoRetry(method, uri, body).retryWhen(errors -> {
      Observable<Throwable> o = errors.flatMap(error -> {
        if (error instanceof HttpException) {
          // immediately forward HTTP errors, don't retry
          return Observable.error(error);
        }
        return Observable.just(error);
      });
      return RxUtils.makeRetry(5, 1000, log).call(o);
    });
  }

  /**
   * Perform an HTTP request and convert the response to a JSON object. Select
   * any host and do not retry on failure.
   * @param method the HTTP method
   * @param uri the request URI
   * @param body the body to send in the request (may be {@code null})
   * @return a single emitting the parsed response body (may be
   * {@code null} if no body was received)
   */
  public Single<JsonObject> performRequestNoRetry(HttpMethod method, String uri, Buffer body) {
    return Single.defer(() -> {
      HttpClient client = nextClient();
      HttpClientRequest req = client.request(method, uri);
      return performRequest(req, body);
    });
  }

  /**
   * Closes this client and all underlying clients
   */
  public void close() {
    hosts.clear();
    for (HttpClient client : hostsToClients.values()) {
      client.close();
    }
    hostsToClients.clear();
    currentHost = -1;
  }
}
