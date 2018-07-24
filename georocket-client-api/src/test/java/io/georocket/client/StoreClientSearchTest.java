package io.georocket.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Tests {@link StoreClient#search(String, String, io.vertx.core.Handler)}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class StoreClientSearchTest extends StoreClientTestBase {
  private Handler<SearchResult> assertExport(String url, String XML,
      TestContext context, Async async) {
    return res -> {
      Buffer response = Buffer.buffer();
      ReadStream<Buffer> r = res.getResponse();
      r.handler(response::appendBuffer);
      r.endHandler(v -> {
        context.assertEquals(XML, response.toString());
        verifyRequested(url, context);
        async.complete();
      });
    };
  }
  
  /**
   * Test if the root layer can be exported
   * @param context the test context
   */
  @Test
  public void exportRoot(TestContext context) {
    String XML = "<test></test>";
    String url = "/store/";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    client.getStore().search(new SearchOptions().setLayer("/"),
        context.asyncAssertSuccess(assertExport(url, XML, context, async)));
  }

  /**
   * Test if a layer can be exported
   * @param context the test context
   */
  @Test
  public void exportLayer(TestContext context) {
    String XML = "<test></test>";
    String url = "/store/hello/world/";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    client.getStore().search(new SearchOptions().setLayer("/hello/world"),
        context.asyncAssertSuccess(assertExport(url, XML, context, context.async())));
  }
  
  /**
   * Test a simple query
   * @param context the test context
   */
  @Test
  public void simpleQuery(TestContext context) {
    String XML = "<test></test>";
    String url = "/store/?search=test";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    client.getStore().search(new SearchOptions().setQuery("test"),
        context.asyncAssertSuccess(assertExport(url, XML, context, context.async())));
  }

  /**
   * Test a query with two terms
   * @param context the test context
   */
  @Test
  public void twoTermsQuery(TestContext context) {
    String XML = "<test></test>";
    String url = "/store/?search=test1+test2";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    client.getStore().search(new SearchOptions().setQuery("test1 test2"),
        context.asyncAssertSuccess(assertExport(url, XML, context, context.async())));
  }
  
  /**
   * Test a query with a layer
   * @param context the test context
   */
  @Test
  public void layer(TestContext context) {
    String XML = "<test></test>";
    String url = "/store/hello/world/?search=test";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    client.getStore().search(new SearchOptions().setQuery("test").setLayer("hello/world"),
        context.asyncAssertSuccess(assertExport(url, XML, context, context.async())));
  }

  /**
   * Test a query with a layer containing special chars
   * @param context the test context
   */
  @Test
  public void layerWithSpecialChars(TestContext context) {
    String XML = "<test></test>";
    String url = "/store/he%2Bllo/world/?search=test";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    client.getStore().search(new SearchOptions().setQuery("test").setLayer("he+llo/world"),
        context.asyncAssertSuccess(assertExport(url, XML, context, context.async())));
  }
}
