package io.georocket.commands;

import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Before;
import org.junit.Rule;

import com.github.tomakehurst.wiremock.client.VerificationException;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.StandardInputReader;
import io.georocket.ConfigConstants;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;

/**
 * Base class for unit tests that test commands
 * @author Michel Kraemer
 * @param <T> the type of the command under test
 */
public abstract class CommandTestBase<T extends AbstractGeoRocketCommand> {
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
   * The command under test
   */
  protected T cmd;
  
  protected InputReader in = new StandardInputReader();
  protected StringWriter writer;
  protected PrintWriter out;
  
  /**
   * Create a new instance of the command under test
   * @return the command under test
   */
  protected abstract T createCommand();
  
  /**
   * Set up test
   * @throws Exception if something goes wrong
   */
  @Before
  public void setUp() throws Exception {
    cmd = createCommand();
    JsonObject config = new JsonObject()
        .put(ConfigConstants.HOST, "localhost")
        .put(ConfigConstants.PORT, PORT);
    cmd.setConfig(config);
    cmd.setVertx(rule.vertx());
    
    writer = new StringWriter();
    out = new PrintWriter(writer);
  }
  
  /**
   * Verify that a certain request has been made
   * @param url the request URL
   * @param context the current test context
   */
  protected void verifyRequested(String url, TestContext context) {
    try {
      verify(getRequestedFor(urlEqualTo(url)));
    } catch (VerificationException e) {
      context.fail(e);
    }
  }
}
