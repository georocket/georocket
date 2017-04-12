package io.georocket.client;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;

/**
 * Tests {@link StoreClient#search(String, String, io.vertx.core.Handler)}
 * @author Tim Hellhake
 */
@RunWith(VertxUnitRunner.class)
public class StoreClientUpdateTest extends StoreClientTestBase {
  /**
   * Test if tags can be can be appended
   * @param context the test context
   */
  @Test
  public void appendTags(TestContext context) {
    String url = "/tags/?search=test&tags=a,b,c";
    stubFor(put(urlEqualTo(url))
      .willReturn(aResponse()
        .withStatus(204)));
    List<String> tags = Arrays.asList("a", "b", "c");
    Async async = context.async();
    client.getStore().appendTags("test", "/", tags, ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }

  /**
   * Test if tags can be can be removed
   * @param context the test context
   */
  @Test
  public void removeTags(TestContext context) {
    String url = "/tags/?search=test&tags=a,b,c";
    stubFor(delete(urlEqualTo(url))
      .willReturn(aResponse()
        .withStatus(204)));
    List<String> tags = Arrays.asList("a", "b", "c");
    Async async = context.async();
    client.getStore().removeTags("test", "/", tags, ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }

  /**
   * Test if properties can be can be set
   * @param context the test context
   */
  @Test
  public void setProperties(TestContext context) {
    String url = "/properties/?search=test&properties=a%3D1,b%3D2";
    stubFor(put(urlEqualTo(url))
      .willReturn(aResponse()
        .withStatus(204)));
    List<String> tags = Arrays.asList("a=1", "b=2");
    Async async = context.async();
    client.getStore().setProperties("test", "/", tags, ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }

  /**
   * Test if properties can be can be removed
   * @param context the test context
   */
  @Test
  public void removeProperties(TestContext context) {
    String url = "/properties/?search=test&properties=a%3D1,b%3D2";
    stubFor(delete(urlEqualTo(url))
      .willReturn(aResponse()
        .withStatus(204)));
    List<String> tags = Arrays.asList("a=1", "b=2");
    Async async = context.async();
    client.getStore().removeProperties("test", "/", tags, ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }
}