package io.georocket.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.github.tomakehurst.wiremock.client.VerificationException;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Tests {@link StoreClient#delete(String, String, io.vertx.core.Handler)}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class StoreClientDeleteTest extends StoreClientTestBase {
  /**
   * Verify that a certain DELETE request has been made
   * @param url the request URL
   * @param context the current test context
   */
  protected void verifyDeleted(String url, TestContext context) {
    try {
      verify(deleteRequestedFor(urlEqualTo(url)));
    } catch (VerificationException e) {
      context.fail(e);
    }
  }
  
  /**
   * Test no layer
   * @param context the test context
   */
  @Test
  public void noLayer(TestContext context) {
    Async async = context.async();
    client.getStore().delete(null, null, context.asyncAssertFailure(t -> {
      context.assertTrue(t instanceof IllegalArgumentException);
      async.complete();
    }));
  }
  
  /**
   * Test empty query
   * @param context the test context
   */
  @Test
  public void emptyQuery(TestContext context) {
    Async async = context.async();
    client.getStore().delete("", null, context.asyncAssertFailure(t -> {
      context.assertTrue(t instanceof IllegalArgumentException);
      async.complete();
    }));
  }
  
  /**
   * Test a delete with a simple query
   * @param context the test context
   */
  @Test
  public void simpleQueryDelete(TestContext context) {
    String url = "/store/?search=test";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    client.getStore().delete("test", context.asyncAssertSuccess(v -> {
      verifyDeleted(url, context);
      async.complete();
    }));
  }
  
  /**
   * Test a delete with query that consists of two terms
   * @param context the test context
   */
  @Test
  public void twoTermsQueryDelete(TestContext context) {
    String url = "/store/?search=test1+test2";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    client.getStore().delete("test1 test2", context.asyncAssertSuccess(v -> {
      verifyDeleted(url, context);
      async.complete();
    }));
  }
  
  /**
   * Test to delete the root layer
   * @param context the test context
   */
  @Test
  public void rootLayer(TestContext context) {
    String url = "/store/";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    client.getStore().delete(null, "/", context.asyncAssertSuccess(v -> {
      verifyDeleted(url, context);
      async.complete();
    }));
  }
  
  /**
   * Test a delete with a layer but no query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void layerNoQuery(TestContext context) throws Exception {
    String url = "/store/hello/world/";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    client.getStore().delete(null, "hello/world", context.asyncAssertSuccess(v -> {
      verifyDeleted(url, context);
      async.complete();
    }));
  }
  
  /**
   * Test a delete with a layer and a query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void layerQuery(TestContext context) throws Exception {
    String url = "/store/hello/world/?search=test";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    client.getStore().delete("test", "hello/world", context.asyncAssertSuccess(v -> {
      verifyDeleted(url, context);
      async.complete();
    }));
  }
}
