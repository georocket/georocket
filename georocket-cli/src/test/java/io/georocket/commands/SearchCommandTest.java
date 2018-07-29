package io.georocket.commands;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test for {@link SearchCommand}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class SearchCommandTest extends CommandTestBase<SearchCommand> {
  @Override
  protected SearchCommand createCommand() {
    return new SearchCommand();
  }
  
  /**
   * Test no query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void noQuery(TestContext context) throws Exception {
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(1, exitCode);
      async.complete();
    });
    context.assertEquals(1, cmd.run(new String[] { }, in, out));
  }
  
  /**
   * Test empty query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void emptyQuery(TestContext context) throws Exception {
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(1, exitCode);
      async.complete();
    });
    context.assertEquals(1, cmd.run(new String[] { "" }, in, out));
  }
  
  /**
   * Test a simple query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void simpleQuery(TestContext context) throws Exception {
    String XML = "<test></test>";
    String url = "/store/?search=test";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      context.assertEquals(XML, writer.toString());
      verifyRequested(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "test" }, in, out);
  }

  /**
   * Test a query with two terms
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void twoTermsQuery(TestContext context) throws Exception {
    String XML = "<test></test>";
    String url = "/store/?search=test1%20test2";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      context.assertEquals(XML, writer.toString());
      verifyRequested(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "test1", "test2" }, in, out);
  }
  
  /**
   * Test a query with a layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void layer(TestContext context) throws Exception {
    String XML = "<test></test>";
    String url = "/store/hello/world/?search=test";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      context.assertEquals(XML, writer.toString());
      verifyRequested(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "-l", "hello/world", "test" }, in, out);
  }
}
