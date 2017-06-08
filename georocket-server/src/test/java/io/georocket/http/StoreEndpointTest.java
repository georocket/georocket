package io.georocket.http;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.rxjava.core.Vertx;
import io.georocket.http.mocks.MockIndexer;
import rx.Observable;

/**
 * Test class for {@link StoreEndpoint}
 * @author David Gengenbach, Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
public class StoreEndpointTest {
  private static Vertx vertx;

  private static io.vertx.core.Vertx vertxCore;

  /**
   * Removes the warnings about blocked threads.
   * Otherwise vertx would log a lot of warnings, because the startup takes some time. 
   */
  private static VertxOptions vertxOptions = new VertxOptions().setBlockedThreadCheckInterval(999999L);

  @ClassRule
  public static RunTestOnContext rule = new RunTestOnContext(vertxOptions);

  /**
   * Starts a MockServer verticle with a StoreEndpoint to test against
   * @param context Test context
   */
  @BeforeClass
  public static void setupServer(TestContext context) {
    Async async = context.async();
    vertx = new Vertx(rule.vertx());
    vertxCore = vertx.getDelegate();

    setConfig(vertx.getOrCreateContext().config());
    setupMockEndpoint().subscribe(x -> async.complete());
  }

  @After
  public void teardown(TestContext context) {
    MockIndexer.unsubscribeIndexer();
  }
  
  /**
   * Tests that a scroll request can be done.
   * @param context Test context
   */
  @Test
  public void testScrolling(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    
    doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=true&size=100", true, true, response -> {
      context.assertEquals(MockIndexer.FIRST_RETURNED_SCROLL_ID, response.getHeader(HeaderConstants.SCROLL_ID));
      checkGeoJsonResponse(response, context, returned -> {
        checkGeoJsonSize(context, response, returned, MockIndexer.HITS_PER_PAGE, true, "The size of the returned elements on the first page should be the page size.");
        async.complete();
      });
    });
  }
  
  /**
   * Tests whether a scroll can be continued with a given scrollId.
   * @param context Test context
   */
  @Test
  public void testScrollingWithGivenScrollId(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=true&scrollId=" + MockIndexer.FIRST_RETURNED_SCROLL_ID, true, true, response -> {
      context.assertEquals(MockIndexer.INVALID_SCROLLID, response.getHeader(HeaderConstants.SCROLL_ID), "The second scrollId should be invalid if there a no elements left.");
      checkGeoJsonResponse(response, context, returned -> {
        checkGeoJsonSize(context, response, returned, MockIndexer.TOTAL_HITS - MockIndexer.HITS_PER_PAGE, true, "The size of the returned elements on the second page should be (TOTAL_HITS - HITS_PER_PAGE)");
        async.complete();
      });
    });
  }
  
  /**
   * Tests what happens when an invalid scrollId is returned.
   * @param context Test context
   */
  @Test
  public void testScrollingWithInvalidScrollId(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    
    doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=true&scrollId=" + MockIndexer.INVALID_SCROLLID, false, false, response -> {
      context.assertEquals(404, response.statusCode(), "Giving an invalid scrollId should return 404.");
      async.complete();
    });
  }

  /**
   * Tests that a normal query returns all the elements.
   * @param context Test context
   */
  @Test
  public void testNormalGet(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=false", false, false, response -> {
      checkGeoJsonResponse(response, context, returned -> {
        checkGeoJsonSize(context, response, returned, MockIndexer.TOTAL_HITS, false, "The size of the returned elements on a normal query should be TOTAL_HITS");
        async.complete();
      });
    });
  }
  /**
   * Tests that a normal query returns all the elements.
   * @param context Test context
   */
  @Test
  public void testNormalGetWithoutScrollParameterGiven(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY", false, false, response -> {
      checkGeoJsonResponse(response, context, returned -> {
        checkGeoJsonSize(context, response, returned, MockIndexer.TOTAL_HITS, false, "The size of the returned elements on a normal query should be TOTAL_HITS");
        async.complete();
      });
    });
  }
  
  private void checkGeoJsonSize(TestContext context, HttpClientResponse response, JsonObject returned, Long expectedSize, boolean checkScrollHeaders, String msg) {
    context.assertEquals(expectedSize, new Long(returned.getJsonArray("geometries").size()), msg == null ? "Response GeoJson had not the expected size!" : msg);
    if (checkScrollHeaders) {
      context.assertEquals(MockIndexer.TOTAL_HITS.toString(), response.getHeader(HeaderConstants.TOTAL_HITS));
      context.assertEquals(expectedSize.toString(), response.getHeader(HeaderConstants.HITS));
    }
  }
  
  private void checkGeoJsonResponse(HttpClientResponse response, TestContext context, Handler<JsonObject> handler) {
    response.bodyHandler(body -> {
      JsonObject returned = body.toJsonObject();
      context.assertNotNull(returned);
      context.assertTrue(returned.containsKey("geometries"));
      handler.handle(returned);
    });
  }
  
  /**
   * Checks for scroll-specific headers that are returned from the server are present or not.
   * @param response client response
   * @param context context
   * @param checkScrollIdHeaderPresent should the test check the scroll id
   */
  private void checkScrollingResponsePresent(HttpClientResponse response, TestContext context, Boolean checkScrollIdHeaderPresent) {
    List<String> neededHeaders = new LinkedList<>();
    neededHeaders.add(HeaderConstants.TOTAL_HITS);
    neededHeaders.add(HeaderConstants.HITS);
    
    if (checkScrollIdHeaderPresent) {
      neededHeaders.add(HeaderConstants.SCROLL_ID);
    }
    
    for (String header : neededHeaders) {
      context.assertNotNull(response.getHeader(header), header + " header not set");
    }
  }
  
  /**
   * Performs request against the server and checks for the scroll headers.
   * Fails when the headers are not present or an error occured during the request.
   *  
   * @param context Test context
   * @param url url
   * @param checkHeaders should the test check the headers
   * @param checkScrollIdHeaderPresent should the test check the scroll id
   * @param handler response handler
   */
  private void doScrolledStorepointRequest(TestContext context, String url, Boolean checkHeaders, 
    Boolean checkScrollIdHeaderPresent, Handler<HttpClientResponse> handler) {
    HttpClient client = createHttpClient();
    HttpClientRequest request = client.get(url, response -> {
      if (checkHeaders) {
        checkScrollingResponsePresent(response, context, checkScrollIdHeaderPresent);
      }
      handler.handle(response);
    });
    request.exceptionHandler(x -> {
      context.fail("Exception during query.");
    });
    request.end();
  }
  
  /**
   * Creates a StoreEndpoint router
   * @return Router
   */
  private static Router getStoreEndpointRouter() {
    Router router = Router.router(vertxCore);
    Endpoint storeEndpoint = new StoreEndpoint(vertxCore);
    router.mountSubRouter("/", storeEndpoint.createRouter());
    return router;
  }

  /**
   * Creates a HttpClient to do requests against the server. No SSL is used.
   * @return a client that's preconfigured for requests to the server.
   */
  private static HttpClient createHttpClient() {
    HttpClientOptions options = new HttpClientOptions()
        .setDefaultHost(vertx.getOrCreateContext().config().getString(ConfigConstants.HOST))
        .setDefaultPort(vertx.getOrCreateContext().config().getInteger(ConfigConstants.PORT))
        .setSsl(false);
    return vertxCore.createHttpClient(options);
  }

  private static Observable<HttpServer> setupMockEndpoint() {
    JsonObject config = vertx.getOrCreateContext().config();
    String host = config.getString(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST);
    int port = config.getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);

    HttpServerOptions serverOptions = new HttpServerOptions().setCompressionSupported(true);
    HttpServer server = vertxCore.createHttpServer(serverOptions);

    ObservableFuture<HttpServer> observable = RxHelper.observableFuture();
    server.requestHandler(getStoreEndpointRouter()::accept).listen(port, host, observable.toHandler());
    return observable;
  }

  private static void setConfig(JsonObject config) {
    // Use mock store
    config.put(ConfigConstants.STORAGE_CLASS, "io.georocket.http.mocks.MockStore");
    config.put(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST);
    config.put(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);
  }

  private final class HeaderConstants {
    static final String SCROLL_ID = "X-Scroll-Id";
    static final String TOTAL_HITS = "X-Total-Hits";
    static final String HITS = "X-Hits";
  }
}
