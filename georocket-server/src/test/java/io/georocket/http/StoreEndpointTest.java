package io.georocket.http;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.constants.ConfigConstants;
import io.georocket.constants.HeaderConstants;
import io.georocket.mocks.MockIndexer;
import io.georocket.mocks.MockServer;
import io.vertx.core.DeploymentOptions;
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
import rx.Observable;

@RunWith(VertxUnitRunner.class)
public class StoreEndpointTest {
  //private static Logger log = LoggerFactory.getLogger(StoreEndpointTest.class);

  static Endpoint endpoint;
  static Vertx vertx;

  static io.vertx.core.Vertx v;
  
  
  /**
   * Removes the warnings about blocked threads.
   * Otherwise vertx would log a lot of warnings, because the startup takes some time. 
   */
  static VertxOptions vertxOptions = new VertxOptions().setBlockedThreadCheckInterval(999999L);

  @ClassRule
  public static RunTestOnContext rule = new RunTestOnContext(vertxOptions);

  /**
   * Starts a MockServer verticle with a StoreEndpoint to test against.
   * @param context
   */
  @BeforeClass
  public static void setupServer(TestContext context) {
    Async async = context.async();
    vertx = new Vertx(rule.vertx());
    v = (io.vertx.core.Vertx)vertx.getDelegate();
    
    vertx.deployVerticle(MockServer.class.getName(), new DeploymentOptions().setWorker(true), context.asyncAssertSuccess(id -> {
      setConfig(getVertxConfig());
      setupMockEndpoint().subscribe(x -> async.complete());
    }));
  }

  @After
  public void teardown(TestContext context) {
    MockIndexer.unsubscribeIndexer();
  }
  
  /**
   * Tests that a paginated request can be done.
   * @param context
   */
  @Test
  public void testPagination(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    
    doPaginatedStorepointRequest(context, "/?search=DUMMY_QUERY&paginated=true", true, response -> {
      context.assertEquals(MockIndexer.FIRST_RETURNED_SCROLL_ID, response.getHeader(HeaderConstants.SCROLL_ID));
      response.bodyHandler(body -> {
          JsonObject returned = body.toJsonObject();
          context.assertEquals(MockIndexer.HITS_PER_PAGE, new Long(returned.getJsonArray("geometries").size()), "The size of the returned elements should be the page size.");
          async.complete();
      });
    });
  }
  
  /**
   * Tests whether a pagination can be continued with a given scrollId.
   * @param context
   */
  @Test
  public void testPaginationWithGivenScrollId(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    doPaginatedStorepointRequest(context, "/?search=DUMMY_QUERY&paginated=true&scrollId=" + MockIndexer.FIRST_RETURNED_SCROLL_ID, true, response -> {
      context.assertEquals(MockIndexer.INVALID_SCROLLID, response.getHeader(HeaderConstants.SCROLL_ID), "The second scrollId should be invalid if there a no elements left.");
      response.bodyHandler(body -> {
        JsonObject returned = body.toJsonObject();
        context.assertNotNull(returned);
        context.assertTrue(returned.containsKey("geometries"));
        context.assertEquals(MockIndexer.TOTAL_HITS - MockIndexer.HITS_PER_PAGE, new Long(returned.getJsonArray("geometries").size()), "The size of the returned elements should be (TOTAL_HITS - HITS_PER_PAGE)");
        async.complete();
      });
    });
  }
  
  @Test
  public void testPaginationWithInvalidScrollId(TestContext context) {
    Async async = context.async();
    MockIndexer.mockIndexerQuery(vertx);
    
    doPaginatedStorepointRequest(context, "/?search=DUMMY_QUERY&paginated=true&scrollId=" + MockIndexer.INVALID_SCROLLID, false, response -> {
      context.assertEquals(MockIndexer.INVALID_SCROLLID, response.getHeader(HeaderConstants.SCROLL_ID), "The returned scrollId should be invalid if an invalid scrollId is given.");
      async.complete();
    });
  }
  
  /**
   * Checks for pagination-specific headers that are returned from the server 
   * @param response
   * @param context
   */
  private void checkPaginatedResponse(HttpClientResponse response, TestContext context, Boolean checkScrollIdHeaderPresent) {
    List<String> neededHeaders = new LinkedList<>();
    neededHeaders.add(HeaderConstants.TOTAL_HITS);
    neededHeaders.add(HeaderConstants.HITS);
    neededHeaders.add(HeaderConstants.PAGE_SIZE);
    
    if (checkScrollIdHeaderPresent) {
      neededHeaders.add(HeaderConstants.SCROLL_ID);
    }
    
    for (String header : neededHeaders) {
      context.assertNotNull(response.getHeader(header), header + " header not set");
    }
  }
  
  /**
   * Performs request against the server and checks for the pagination headers.
   * Fails when the headers are not present or an error occured during the request.
   *  
   * @param context
   * @param url
   * @param handler
   */
  private void doPaginatedStorepointRequest(TestContext context, String url, Boolean checkScrollIdHeaderPresent, Handler<HttpClientResponse> handler) {
    HttpClient client = createHttpClient();
    HttpClientRequest request = client.get(url, response -> {
      checkPaginatedResponse(response, context, checkScrollIdHeaderPresent);
      handler.handle(response);
    });
    request.exceptionHandler(x -> {
      context.fail("Exception during query.");
    });
    request.end();
  }
  
  /**
   * Creates a StoreEndpoint router
   */
  private static Router getStoreEndpointRouter() {
    Router router = Router.router(v);
    Endpoint storeEndpoint = new StoreEndpoint(v);
    router.mountSubRouter("/", storeEndpoint.createRouter());
    return router;
  }

  /**
   * Creates a HttpClient to do requests against the server. No SSL is used.
   * @return a client that's preconfigured for requests to the server.
   */
  private HttpClient createHttpClient() {
    HttpClientOptions options = new HttpClientOptions()
        .setDefaultHost(getVertxConfig().getString(ConfigConstants.HOST))
        .setDefaultPort(getVertxConfig().getInteger(ConfigConstants.PORT))
        .setSsl(false);
    return v.createHttpClient(options);
  }


  private static Observable<HttpServer> setupMockEndpoint() {
    return MockServer.deployHttpServer(v, getVertxConfig(), getStoreEndpointRouter());
  }
  
  private static JsonObject getVertxConfig() {
    return vertx.getOrCreateContext().config();
  }
  
  protected static void setConfig(JsonObject config) {
    // Use mock store
    config.put(ConfigConstants.STORAGE_CLASS, "io.georocket.mocks.MockStore");
    config.put(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST);
    config.put(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);
    config.put(ConfigConstants.PAGINATION_ENABLED, true);
  }
}
