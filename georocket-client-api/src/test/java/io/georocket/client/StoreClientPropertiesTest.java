package io.georocket.client;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

/**
 * Tests {@link StoreClient}
 * @author Tim Hellhake
 */
@RunWith(VertxUnitRunner.class)
public class StoreClientPropertiesTest extends StoreClientTestBase {
  /**
   * Test if properties can be can be set
   * @param context the test context
   */
  @Test
  public void setProperties(TestContext context) {
    String url = "/store/?search=test&properties=a%3D1,b%3D2";
    stubFor(put(urlEqualTo(url))
      .willReturn(aResponse()
        .withStatus(204)));
    List<String> tags = Arrays.asList("a=1", "b=2");
    Async async = context.async();
    client.getStoreClient().setProperties("test", "/", tags, ar -> {
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
    String url = "/store/?search=test&properties=a%3D1,b%3D2";
    stubFor(delete(urlEqualTo(url))
      .willReturn(aResponse()
        .withStatus(204)));
    List<String> tags = Arrays.asList("a=1", "b=2");
    Async async = context.async();
    client.getStoreClient().removeProperties("test", "/", tags, ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }
}
