package io.georocket.index.elasticsearch;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.binaryEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/**
 * Tests for {@link LoadBalancingHttpClient}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class LoadBalancingHttpClientTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  /**
   * Mock server 1
   */
  @Rule
  public WireMockRule wireMockRule1 = new WireMockRule(options().dynamicPort());

  /**
   * Mock server 2
   */
  @Rule
  public WireMockRule wireMockRule2 = new WireMockRule(options().dynamicPort());

  /**
   * Mock server 3
   */
  @Rule
  public WireMockRule wireMockRule3 = new WireMockRule(options().dynamicPort());

  /**
   * Mock server 4
   */
  @Rule
  public WireMockRule wireMockRule4 = new WireMockRule(options().dynamicPort());

  private final JsonObject expected1 = new JsonObject().put("name", "D'Artagnan");
  private final JsonObject expected2 = new JsonObject().put("name", "Athos");
  private final JsonObject expected3 = new JsonObject().put("name", "Porthos");
  private final JsonObject expected4 = new JsonObject().put("name", "Aramis");

  /**
   * The client under test
   */
  private LoadBalancingHttpClient client;

  @Before
  public void setUp() {
    client = new LoadBalancingHttpClient(rule.vertx());

    wireMockRule1.stubFor(get(urlEqualTo("/"))
      .willReturn(aResponse()
        .withBody(expected1.encode())));
    wireMockRule2.stubFor(get(urlEqualTo("/"))
      .willReturn(aResponse()
        .withBody(expected2.encode())));
    wireMockRule3.stubFor(get(urlEqualTo("/"))
      .willReturn(aResponse()
        .withBody(expected3.encode())));
    wireMockRule4.stubFor(get(urlEqualTo("/"))
      .willReturn(aResponse()
        .withBody(expected4.encode())));
  }

  @After
  public void tearDown() {
    client.close();
  }

  /**
   * Test if four requests can be sent to four hosts
   */
  @Test
  public void fourHostsFourRequests(TestContext ctx) {
    client.setHosts(Arrays.asList(
        URI.create("http://localhost:" + wireMockRule1.port()),
        URI.create("http://localhost:" + wireMockRule2.port()),
        URI.create("http://localhost:" + wireMockRule3.port()),
        URI.create("http://localhost:" + wireMockRule4.port())));

    Async async = ctx.async();
    client.performRequest("/")
      .doOnSuccess(o -> ctx.assertEquals(expected1, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected2, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected3, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected4, o))
      .subscribe(o -> async.complete());
  }

  /**
   * Test if six requests can be sent to four hosts
   */
  @Test
  public void fourHostsSixRequests(TestContext ctx) {
    client.setHosts(Arrays.asList(
      URI.create("http://localhost:" + wireMockRule1.port()),
      URI.create("http://localhost:" + wireMockRule2.port()),
      URI.create("http://localhost:" + wireMockRule3.port()),
      URI.create("http://localhost:" + wireMockRule4.port())));

    Async async = ctx.async();
    client.performRequest("/")
      .doOnSuccess(o -> ctx.assertEquals(expected1, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected2, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected3, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected4, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected1, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected2, o))
      .subscribe(o -> async.complete());
  }

  /**
   * Test if five requests can be sent to three hosts in a pseudo-random order
   */
  @Test
  public void anotherOrder(TestContext ctx) {
    client.setHosts(Arrays.asList(
      URI.create("http://localhost:" + wireMockRule4.port()),
      URI.create("http://localhost:" + wireMockRule1.port()),
      URI.create("http://localhost:" + wireMockRule3.port())));

    Async async = ctx.async();
    client.performRequest("/")
      .doOnSuccess(o -> ctx.assertEquals(expected4, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected1, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected3, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected4, o))
      .flatMap(v -> client.performRequest("/"))
      .doOnSuccess(o -> ctx.assertEquals(expected1, o))
      .subscribe(o -> async.complete());
  }

  /**
   * Test what happens if the first host is unreachable
   */
  @Test
  public void retrySecondHost(TestContext ctx) {
    client.setDefaultOptions(new HttpClientOptions().setConnectTimeout(500));
    client.setHosts(Arrays.asList(
      URI.create("http://192.0.2.0:80"),
      URI.create("http://localhost:" + wireMockRule2.port())));

    Async async = ctx.async();
    client.performRequest("/")
      .doOnSuccess(o -> ctx.assertEquals(expected2, o))
      .subscribe(o -> async.complete());
  }

  /**
   * Test if request bodies can be compressed
   */
  @Test
  public void compressRequestBodies(TestContext ctx) {
    client.close();
    client = new LoadBalancingHttpClient(rule.vertx(), true);

    Buffer body = Buffer.buffer();
    for (int i = 0; i < 150; ++i) {
      body.appendString("Hello world");
    }

    wireMockRule1.stubFor(post(urlEqualTo("/"))
      .withHeader("Content-Encoding", equalTo("gzip"))
      .withRequestBody(binaryEqualTo(body.getBytes()))
      .willReturn(aResponse()
        .withBody(expected1.encode())));

    client.setHosts(Collections.singletonList(
      URI.create("http://localhost:" + wireMockRule1.port())));

    Async async = ctx.async();
    client.performRequest(HttpMethod.POST, "/", body)
      .doOnSuccess(o -> ctx.assertEquals(expected1, o))
      .subscribe(o -> async.complete());
  }

  /**
   * Make sure request bodies that are too small are not compressed
   */
  @Test
  public void compressRequestBodiesMessageTooSmall(TestContext ctx) {
    client.close();
    client = new LoadBalancingHttpClient(rule.vertx(), true);

    Buffer body = Buffer.buffer("Hello World");

    wireMockRule1.stubFor(post(urlEqualTo("/"))
      .withHeader("Content-Encoding", absent())
      .withRequestBody(binaryEqualTo(body.getBytes()))
      .willReturn(aResponse()
        .withBody(expected1.encode())));

    client.setHosts(Collections.singletonList(
      URI.create("http://localhost:" + wireMockRule1.port())));

    Async async = ctx.async();
    client.performRequest(HttpMethod.POST, "/", body)
      .doOnSuccess(o -> ctx.assertEquals(expected1, o))
      .subscribe(o -> async.complete());
  }
}
