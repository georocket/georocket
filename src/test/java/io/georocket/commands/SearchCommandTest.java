package io.georocket.commands;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.github.tomakehurst.wiremock.client.VerificationException;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.StandardInputReader;
import io.georocket.ConfigConstants;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test for {@link SearchCommand}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class SearchCommandTest {
  private static final int PORT = 12345;

  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  /**
   * Run a mock HTTP server
   */
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(PORT);
  
  /**
   * The object under test
   */
  private SearchCommand sc;
  
  private InputReader in = new StandardInputReader();
  private StringWriter writer;
  private PrintWriter out;
  
  /**
   * Set up test
   */
  @Before
  public void setUp() {
    sc = new SearchCommand();
    JsonObject config = new JsonObject()
        .put(ConfigConstants.HOST, "localhost")
        .put(ConfigConstants.PORT, PORT);
    sc.setConfig(config);
    sc.setVertx(rule.vertx());
    
    writer = new StringWriter();
    out = new PrintWriter(writer);
  }
  
  /**
   * Test no query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void noQuery(TestContext context) throws Exception {
    context.assertEquals(1, sc.run(new String[] { }, in, out));
  }
  
  /**
   * Test empty query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void emptyQuery(TestContext context) throws Exception {
    context.assertEquals(1, sc.run(new String[] { "" }, in, out));
  }
  
  private void verifyRequested(String url, TestContext context) {
    try {
      verify(getRequestedFor(urlEqualTo(url)));
    } catch (VerificationException e) {
      context.fail(e);
    }
  }
  
  /**
   * Test a simple query
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void simpleQuery(TestContext context) throws Exception {
    String XML = "<test></test>";
    stubFor(get(urlEqualTo("/store/?search=test"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    sc.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      context.assertEquals(XML, writer.toString());
      verifyRequested("/store/?search=test", context);
      async.complete();
    });
    
    sc.run(new String[] { "test" }, in, out);
  }

  /**
   * Test a query with two terms
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void twoTermsQuery(TestContext context) throws Exception {
    String XML = "<test></test>";
    stubFor(get(urlEqualTo("/store/?search=test1+test2"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    sc.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      context.assertEquals(XML, writer.toString());
      verifyRequested("/store/?search=test1+test2", context);
      async.complete();
    });
    
    sc.run(new String[] { "test1", "test2" }, in, out);
  }
  
  /**
   * Test a query with a layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void layer(TestContext context) throws Exception {
    String XML = "<test></test>";
    stubFor(get(urlEqualTo("/store/hello/world/?search=test"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    sc.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      context.assertEquals(XML, writer.toString());
      verifyRequested("/store/hello/world/?search=test", context);
      async.complete();
    });
    
    sc.run(new String[] { "-l", "hello/world", "test" }, in, out);
  }
}
