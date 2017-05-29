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
 * Tests {@link TagsClient}
 * @author Tim Hellhake
 */
@RunWith(VertxUnitRunner.class)
public class TagsClientTest extends StoreClientTestBase {
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
    client.getTagsClient().appendTags("test", "/", tags, ar -> {
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
    client.getTagsClient().removeTags("test", "/", tags, ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }
}