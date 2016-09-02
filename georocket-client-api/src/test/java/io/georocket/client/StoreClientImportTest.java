package io.georocket.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.github.tomakehurst.wiremock.client.VerificationException;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Tests {@link StoreClient#startImport(String, java.util.Collection, java.util.Optional, io.vertx.core.Handler)}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class StoreClientImportTest extends StoreClientTestBase {
  /**
   * Test XML file contents to import
   */
  private static final String XML = "<test></test>";
  
  /**
   * Verify that a certain POST request has been made
   * @param url the request URL
   * @param body the request body
   * @param context the current test context
   */
  protected void verifyPosted(String url, String body, TestContext context) {
    try {
      verify(postRequestedFor(urlEqualTo(url))
          .withRequestBody(equalTo(body)));
    } catch (VerificationException e) {
      context.fail(e);
    }
  }
  
  /**
   * Test a simple import
   * @param context the test context
   */
  @Test
  public void simpleImport(TestContext context) {
    String url = "/store";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));
    
    Async async = context.async();
    WriteStream<Buffer> w = client.getStore().startImport(
        context.asyncAssertSuccess(v -> {
      verifyPosted(url, XML, context);
      async.complete();
    }));
    w.end(Buffer.buffer(XML));
  }
  
  /**
   * Test importing to a layer
   * @param context the test context
   */
  @Test
  public void importLayer(TestContext context) {
    String url = "/store/hello/world/";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));
    
    Async async = context.async();
    WriteStream<Buffer> w = client.getStore().startImport("hello/world",
        context.asyncAssertSuccess(v -> {
      verifyPosted(url, XML, context);
      async.complete();
    }));
    w.end(Buffer.buffer(XML));
  }

  @Test
  public void importLayerWithSpecialChars(TestContext context) {
    String url = "/store/he%2Bllo/world/";
    stubFor(post(urlEqualTo(url))
            .willReturn(aResponse()
                    .withStatus(202)));

    Async async = context.async();
    WriteStream<Buffer> w = client.getStore().startImport("he+llo/world",
            context.asyncAssertSuccess(v -> {
              verifyPosted(url, XML, context);
              async.complete();
            }));
    w.end(Buffer.buffer(XML));
  }
  
  /**
   * Test importing with tags
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void importTags(TestContext context) throws Exception {
    String url = "/store?tags=hello,world";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));
    
    Async async = context.async();
    WriteStream<Buffer> w = client.getStore().startImport(null,
        Arrays.asList("hello", "world"), context.asyncAssertSuccess(v -> {
      verifyPosted(url, XML, context);
      async.complete();
    }));
    w.end(Buffer.buffer(XML));
  }
}
