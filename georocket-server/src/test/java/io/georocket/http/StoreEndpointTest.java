package io.georocket.http;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.mocks.MockServer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.rxjava.core.Vertx;
import rx.Observable;
import rx.Subscription;

@RunWith(VertxUnitRunner.class)
public class StoreEndpointTest {
  //private static Logger log = LoggerFactory.getLogger(StoreEndpointTest.class);

  static Endpoint endpoint;
  static Vertx vertx;

  static io.vertx.core.Vertx v;
  
  public static Long TOTAL_HITS = 100L;
  public static String RETURNED_SCROLL_ID = "NEW_SCROLL_ID";
  
  static VertxOptions vertxOptions = new VertxOptions().setBlockedThreadCheckInterval(999999L);

  @ClassRule
  public static RunTestOnContext rule = new RunTestOnContext(vertxOptions);

  @BeforeClass
  public static void setupServer(TestContext context) {
    Async async = context.async();
    vertx = new Vertx(rule.vertx());
    v = (io.vertx.core.Vertx) vertx.getDelegate();
    
    vertx.deployVerticle(MockServer.class.getName(), new DeploymentOptions().setWorker(true), context.asyncAssertSuccess(id -> {
      setConfig(getVertxConfig());
      mockIndexerQuery();
      setupMockEndpoint().subscribe(x-> async.complete());
    }));
  }

  @Test
  public void testPagination(TestContext context) {
    System.out.println("testPagination");
    Async async = context.async();
    
    
    HttpClient client = createHttpClient();
    HttpClientRequest request = client.get("/?search=DUMMY_QUERY&paginated=true", response -> {
      checkPaginatedResponse(response, context);
      context.assertEquals(RETURNED_SCROLL_ID, response.getHeader("X-Scroll-Id"));
      async.complete();
    });

    request.exceptionHandler(x -> async.resolve(Future.failedFuture("Exception during query.")));
    request.end();
  }
  
  @Test
  public void testPaginationWithGivenScrollId(TestContext context) {
    System.out.println("testPaginationWithGivenScrollId");
    Async async = context.async();

    HttpClient client = createHttpClient();
    HttpClientRequest request = client.get("/?search=DUMMY_QUERY&paginated=true&scrollId=" + RETURNED_SCROLL_ID, response -> {
      checkPaginatedResponse(response, context);
      context.assertEquals(RETURNED_SCROLL_ID, response.getHeader("X-Scroll-Id"));
      async.complete();
    });
    
    request.exceptionHandler(x -> async.resolve(Future.failedFuture("Exception during query.")));
    request.end();
  }
  
  private void checkPaginatedResponse(HttpClientResponse response, TestContext context) {
    context.assertNotNull(response.getHeader("X-Scroll-Id"), "X-Scroll-Id header not set");
    context.assertNotNull(response.getHeader("X-Total-Hits"), "X-Total-Hits header not set");
    context.assertNotNull(response.getHeader("X-Hits"), "X-Hits header not set");
  }
  
  private static Router createRouter() {
    Router router = Router.router(v);
    Endpoint storeEndpoint = new StoreEndpoint(v);
    router.mountSubRouter("/", storeEndpoint.createRouter());
    return router;
  }

  private HttpClient createHttpClient() {
    HttpClientOptions options = new HttpClientOptions()
        .setDefaultHost(getVertxConfig().getString(ConfigConstants.HOST))
        .setDefaultPort(getVertxConfig().getInteger(ConfigConstants.PORT))
        .setSsl(false);
    return v.createHttpClient(options);
  }

  private static JsonObject getVertxConfig() {
    return vertx.getOrCreateContext().config();
  }

  private static Observable<HttpServer> setupMockEndpoint() {
    return MockServer.deployHttpServer((io.vertx.core.Vertx) vertx.getDelegate(), getVertxConfig(), createRouter());
  }

  private static Subscription mockIndexerQuery() {
    return vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).toObservable()
        .subscribe(msg -> {
          System.out.println("mockIndexerQuery.comsumer");
          msg.reply(new JsonObject()
              .put("totalHits", TOTAL_HITS * 1.5)
              .put("scrollId", RETURNED_SCROLL_ID)
              .put("hits", new JsonArray().add(new JsonObject().put("mimeType", "application/xml").put("id", "<child>Element1</child>").put("start", 0).put("end", 1).put("parents", new JsonArray()))));
        });
  }
  
  protected static void setConfig(JsonObject config) {
    config.put(ConfigConstants.STORAGE_CLASS, "io.georocket.mocks.MockStore");
    config.put(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST);
    config.put(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);
  }
}
