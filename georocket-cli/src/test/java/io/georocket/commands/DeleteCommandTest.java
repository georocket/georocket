package io.georocket.commands;

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
 * Test for {@link DeleteCommand}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class DeleteCommandTest extends CommandTestBase<DeleteCommand> {
  @Override
  protected DeleteCommand createCommand() {
    return new DeleteCommand();
  }
  
  /**
   * Test no layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void noLayer(TestContext context) throws Exception {
    context.assertEquals(1, cmd.run(new String[] { }, in, out));
  }
  
  /**
   * Test empty layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void emptyLayer(TestContext context) throws Exception {
    context.assertEquals(1, cmd.run(new String[] { "" }, in, out));
  }
  
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
   * Test a delete with a simple query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void simpleQueryDelete(TestContext context) throws Exception {
    String url = "/store/?search=test";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyDeleted(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "test" }, in, out);
  }

  /**
   * Test a delete with query that consists of two terms
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void twoTermsQueryDelete(TestContext context) throws Exception {
    String url = "/store/?search=test1+test2";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyDeleted(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "test1", "test2" }, in, out);
  }
  
  /**
   * Test to delete the root layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void rootLayer(TestContext context) throws Exception {
    String url = "/store/";
    stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(204)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyDeleted(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "-l", "/" }, in, out);
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
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyDeleted(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "-l", "hello/world" }, in, out);
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
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyDeleted(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "-l", "hello/world", "test" }, in, out);
  }
}
