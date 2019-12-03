package io.georocket.index.elasticsearch;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.headRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/**
 * Test for {@link RemoteElasticsearchClient}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class RemoteElasticsearchClientTest {
  private static final String INDEX = "myindex";
  private static final String TYPE = "mytype";
  
  private static final JsonObject BULK_RESPONSE_ERROR = new JsonObject()
      .put("errors", true)
      .put("items", new JsonArray()
        .add(new JsonObject()
          .put("index", new JsonObject()
            .put("_id", "A")
            .put("status", 200)))
        .add(new JsonObject()
          .put("index", new JsonObject()
            .put("_id", "B")
            .put("status", 400)
            .put("error", new JsonObject()
              .put("type", "mapper_parsing_exception")
              .put("reason", "Field name [na.me] cannot contain '.'"))))
        .add(new JsonObject()
            .put("index", new JsonObject()
              .put("_id", "C")
              .put("status", 400)
              .put("error", new JsonObject()
                .put("type", "mapper_parsing_exception")
                .put("reason", "Field name [nam.e] cannot contain '.'")))));

  private static final JsonObject SETTINGS = new JsonObject()
    .put("index", new JsonObject()
      .put("number_of_shards", 3)
      .put("number_of_replicas", 3));
  private static final JsonObject SETTINGS_WRAPPER = new JsonObject()
      .put("settings", SETTINGS);

  private static final JsonObject ACKNOWLEDGED = new JsonObject()
      .put("acknowledged", true);

  private ElasticsearchClient client;
  
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  /**
   * Run a mock HTTP server
   */
  @Rule
  public WireMockRule wireMockRule1 = new WireMockRule(options().dynamicPort(), false);

  /**
   * Run another mock HTTP server
   */
  @Rule
  public WireMockRule wireMockRule2 = new WireMockRule(options().dynamicPort(), false);

  /**
   * Create the Elasticsearch client
   */
  @Before
  public void setUp() {
    List<URI> hosts = Collections.singletonList(URI.create("http://localhost:"
        + wireMockRule1.port()));
    client = new RemoteElasticsearchClient(hosts, INDEX, null, false, rule.vertx());
  }
  
  /**
   * Close the Elasticsearch client
   */
  @After
  public void tearDown() {
    client.close();
  }
  
  /**
   * Test if the {@link ElasticsearchClient#indexExists()} method returns
   * <code>false</code> if the index does not exist
   * @param context the test context
   */
  @Test
  public void indexExistsFalse(TestContext context) {
    wireMockRule1.stubFor(head(urlEqualTo("/" + INDEX))
        .willReturn(aResponse()
            .withStatus(404)));
    
    Async async = context.async();
    client.indexExists().subscribe(f -> {
      context.assertFalse(f);
      async.complete();
    }, context::fail);
  }

  /**
   * Test if the {@link ElasticsearchClient#typeExists(String)} method returns
   * <code>false</code> if the index does not exist
   * @param context the test context
   */
  @Test
  public void typeIndexExistsFalse(TestContext context) {
    wireMockRule1.stubFor(head(urlEqualTo("/" + INDEX + "/" + TYPE))
      .willReturn(aResponse()
        .withStatus(404)));

    Async async = context.async();
    client.typeExists(TYPE).subscribe(f -> {
      context.assertFalse(f);
      async.complete();
    }, context::fail);
  }
  
  /**
   * Test if the {@link ElasticsearchClient#indexExists()} method returns
   * <code>true</code> if the index exists
   * @param context the test context
   */
  @Test
  public void indexExistsTrue(TestContext context) {
    wireMockRule1.stubFor(head(urlEqualTo("/" + INDEX))
        .willReturn(aResponse()
            .withStatus(200)));
    
    Async async = context.async();
    client.indexExists().subscribe(f -> {
      context.assertTrue(f);
      async.complete();
    }, context::fail);
  }

  /**
   * Test if the {@link ElasticsearchClient#indexExists()} method returns
   * <code>true</code> if the index exists
   * @param context the test context
   */
  @Test
  public void typeIndexExistsTrue(TestContext context) {
    wireMockRule1.stubFor(head(urlEqualTo("/" + INDEX + "/_mapping/" + TYPE +
          "?include_type_name=true"))
      .willReturn(aResponse()
        .withStatus(200)));

    Async async = context.async();
    client.typeExists(TYPE).subscribe(f -> {
      context.assertTrue(f);
      async.complete();
    }, context::fail);
  }

  /**
   * Test if the client can create a mapping for an index
   * @param context the test context
   */
  @Test
  public void putMapping(TestContext context) {
    wireMockRule1.stubFor(put(urlEqualTo("/" + INDEX + "/_mapping/" + TYPE +
          "?include_type_name=true"))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody(ACKNOWLEDGED.encode())));

    JsonObject mappings = new JsonObject()
      .put("properties", new JsonObject()
        .put("name", new JsonObject()
          .put("type", "text")));

    Async async = context.async();
    client.putMapping(TYPE, mappings).subscribe(ack -> {
      wireMockRule1.verify(putRequestedFor(urlEqualTo("/" + INDEX + "/_mapping/" + TYPE +
            "?include_type_name=true"))
        .withRequestBody(equalToJson("{\"properties\":{\"name\":{\"type\":\"text\"}}}}}")));
      context.assertTrue(ack);
      async.complete();
    }, context::fail);
  }
  
  /**
   * Test if the client can create an index
   * @param context the test context
   */
  @Test
  public void createIndex(TestContext context) {
    StubMapping settings = wireMockRule1.stubFor(put(urlEqualTo("/" + INDEX))
      .willReturn(aResponse()
        .withBody(ACKNOWLEDGED.encode())
        .withStatus(200)));

    Async async = context.async();
    client.createIndex().subscribe(ok -> {
      context.assertTrue(ok);
      wireMockRule1.verify(putRequestedFor(settings.getRequest().getUrlMatcher()));
      async.complete();
    }, context::fail);
  }

  /**
   * Test if the client can create an index with settings
   * @param context the test context
   */
  @Test
  public void createIndexWithSettings(TestContext context) {
    StubMapping settings = wireMockRule1.stubFor(put(urlEqualTo("/" + INDEX))
      .withRequestBody(equalToJson(SETTINGS_WRAPPER.encode()))
      .willReturn(aResponse()
        .withBody(ACKNOWLEDGED.encode())
        .withStatus(200)));

    Async async = context.async();
    client.createIndex(SETTINGS).subscribe(ok -> {
      context.assertTrue(ok);
      wireMockRule1.verify(putRequestedFor(settings.getRequest().getUrlMatcher()));
      async.complete();
    }, context::fail);
  }

  /**
   * Test if the client can insert multiple documents in one request
   * @param context the test context
   */
  @Test
  public void bulkInsert(TestContext context) {
    String url = "/" + INDEX + "/" + TYPE + "/_bulk";

    wireMockRule1.stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody("{}")));
    
    List<Tuple2<String, JsonObject>> documents = new ArrayList<>();
    documents.add(Tuple.tuple("A", new JsonObject().put("name", "Elvis")));
    documents.add(Tuple.tuple("B", new JsonObject().put("name", "Max")));
    
    Async async = context.async();
    client.bulkInsert(TYPE, documents).subscribe(res -> {
      wireMockRule1.verify(postRequestedFor(urlEqualTo(url))
          .withRequestBody(equalToJson("{\"index\":{\"_id\":\"A\"}}\n" + 
              "{\"name\":\"Elvis\"}\n" + 
              "{\"index\":{\"_id\":\"B\"}}\n" + 
              "{\"name\":\"Max\"}\n")));
      context.assertEquals(0, res.size());
      async.complete();
    }, context::fail);
  }
  
  /**
   * Check if {@link ElasticsearchClient#isRunning()} returns false
   * if it is not running
   * @param context the test context
   */
  @Test
  public void isRunningFalse(TestContext context) {
    Async async = context.async();
    client.isRunning().subscribe(r -> {
      context.assertFalse(r);
      async.complete();
    }, context::fail);
  }
  
  /**
   * Check if {@link ElasticsearchClient#isRunning()} returns true
   * if it is running
   * @param context the test context
   */
  @Test
  public void isRunning(TestContext context) {
    wireMockRule1.stubFor(head(urlEqualTo("/"))
        .willReturn(aResponse()
            .withStatus(200)));
    
    Async async = context.async();
    client.isRunning().subscribe(r -> {
      context.assertTrue(r);
      wireMockRule1.verify(headRequestedFor(urlEqualTo("/")));
      async.complete();
    }, context::fail);
  }
  
  /**
   * Test the {@link ElasticsearchClient#bulkResponseHasErrors(JsonObject)} method
   * @param context the test context
   */
  @Test
  public void bulkResponseHasErrors(TestContext context) {
    context.assertTrue(client.bulkResponseHasErrors(BULK_RESPONSE_ERROR));
    context.assertFalse(client.bulkResponseHasErrors(new JsonObject()));
  }
  
  /**
   * Test {@link ElasticsearchClient#bulkResponseGetErrorMessage(JsonObject)}
   * produces an error message
   * @param context the test context
   */
  @Test
  public void bulkResponseGetErrorMessage(TestContext context) {
    context.assertNull(client.bulkResponseGetErrorMessage(new JsonObject()));
    String expected = "Errors in bulk operation:\n"
        + "[id: [B], type: [mapper_parsing_exception], "
        + "reason: [Field name [na.me] cannot contain '.']]\n"
        + "[id: [C], type: [mapper_parsing_exception], "
        + "reason: [Field name [nam.e] cannot contain '.']]";
    context.assertEquals(expected,
        client.bulkResponseGetErrorMessage(BULK_RESPONSE_ERROR));
  }

  /**
   * Test if the client can connect to multiple hosts
   * @param context the test context
   */
  @Test
  public void multipleHosts(TestContext context) {
    List<URI> hosts = Arrays.asList(URI.create("http://localhost:" + wireMockRule1.port()),
        URI.create("http://localhost:" + wireMockRule2.port()));
    client = new RemoteElasticsearchClient(hosts, INDEX, null, false, rule.vertx());

    wireMockRule1.stubFor(head(urlEqualTo("/" + INDEX))
      .willReturn(aResponse()
        .withStatus(200)));
    wireMockRule2.stubFor(head(urlEqualTo("/" + INDEX))
      .willReturn(aResponse()
        .withStatus(404)));

    Async async = context.async();
    client.indexExists()
      .doOnSuccess(context::assertTrue)
      .flatMap(v -> client.indexExists())
      .doOnSuccess(context::assertFalse)
      .subscribe(v -> async.complete(), context::fail);
  }

  /**
   * Test if the client can auto-detect multiple hosts
   * @param context the test context
   */
  @Test
  public void autoDetectHosts(TestContext context) {
    List<URI> hosts = Arrays.asList(URI.create("http://localhost:" + wireMockRule1.port()));
    client.close();
    client = new RemoteElasticsearchClient(hosts, INDEX, Duration.ofMillis(222),
        false, rule.vertx());

    JsonObject nodes = new JsonObject()
      .put("nodes", new JsonObject()
        .put("A", new JsonObject()
          .put("http", new JsonObject()
            .put("publish_address", "localhost:" + wireMockRule1.port())))
        .put("B", new JsonObject()
          .put("http", new JsonObject()
            .put("publish_address", "localhost:" + wireMockRule2.port()))));


    wireMockRule1.stubFor(get(urlEqualTo("/_nodes/http"))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody(nodes.encode())));
    wireMockRule2.stubFor(get(urlEqualTo("/_nodes/http"))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody(nodes.encode())));

    wireMockRule1.stubFor(head(urlEqualTo("/" + INDEX))
      .willReturn(aResponse()
        .withStatus(200)));
    wireMockRule2.stubFor(head(urlEqualTo("/" + INDEX))
      .willReturn(aResponse()
        .withStatus(404)));

    Set<Boolean> expected = new HashSet<>();
    expected.add(Boolean.TRUE);
    expected.add(Boolean.FALSE);
    Async async = context.async(expected.size());
    rule.vertx().setPeriodic(100, id -> {
      client.indexExists()
        .subscribe(e -> {
          if (expected.remove(e)) {
            if (expected.isEmpty()) {
              rule.vertx().cancelTimer(id);
            }
            async.countDown();
          }
        }, context::fail);
    });
  }
}
