package io.georocket.index.elasticsearch;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.headRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;

/**
 * Test for {@link ElasticsearchClient}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class ElasticsearchClientTest {
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
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());
  
  /**
   * Create the Elasticsearch client
   */
  @Before
  public void setUp() {
    configureFor("localhost", wireMockRule.port());
    client = new ElasticsearchClient("localhost", wireMockRule.port(),
        INDEX, new Vertx(rule.vertx()));
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
    stubFor(head(urlEqualTo("/" + INDEX))
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
    stubFor(head(urlEqualTo("/" + INDEX + "/" + TYPE))
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
    stubFor(head(urlEqualTo("/" + INDEX))
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
    stubFor(head(urlEqualTo("/" + INDEX + "/" + TYPE))
      .willReturn(aResponse()
        .withStatus(200)));

    Async async = context.async();
    client.typeExists(TYPE).subscribe(f -> {
      context.assertTrue(f);
      async.complete();
    }, context::fail);
  }

  /**
   * Test if the client can create an index
   * @param context the test context
   */
  @Test
  public void createIndex(TestContext context) {
    stubFor(put(urlEqualTo("/" + INDEX + "/_mapping/" + TYPE))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody("{\"ackowledged\":true}")));

    JsonObject mappings = new JsonObject()
      .put("properties", new JsonObject()
        .put("name", new JsonObject()
          .put("type", "string")));

    Async async = context.async();
    client.createIndex(TYPE, mappings).subscribe(ack -> {
      verify(putRequestedFor(urlEqualTo("/" + INDEX + "/_mapping/" + TYPE))
        .withRequestBody(equalToJson("{\"properties\":{\"name\":{\"type\":\"string\"}}}}}")));
      context.assertTrue(ack);
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
    
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody("{}")));
    
    Map<String, JsonObject> documents = new HashMap<>();
    documents.put("A", new JsonObject().put("name", "Elvis"));
    documents.put("B", new JsonObject().put("name", "Max"));
    
    Async async = context.async();
    client.bulkInsert(TYPE, documents).subscribe(res -> {
      verify(postRequestedFor(urlEqualTo(url))
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
    stubFor(head(urlEqualTo("/"))
        .willReturn(aResponse()
            .withStatus(200)));
    
    Async async = context.async();
    client.isRunning().subscribe(r -> {
      context.assertTrue(r);
      verify(headRequestedFor(urlEqualTo("/")));
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
}
