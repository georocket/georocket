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
 * Test for {@link ExportCommand}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class ExportCommandTest extends CommandTestBase<ExportCommand> {
  @Override
  protected ExportCommand createCommand() {
    return new ExportCommand();
  }
  
  /**
   * Test no layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void noLayer(TestContext context) throws Exception {
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(1, exitCode);
      async.complete();
    });
    cmd.run(new String[] { }, in, out);
  }
  
  /**
   * Test empty layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void emptyLayer(TestContext context) throws Exception {
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(1, exitCode);
      async.complete();
    });
    cmd.run(new String[] { "" }, in, out);
  }
  
  /**
   * Test if the root layer can be exported
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void exportRoot(TestContext context) throws Exception {
    String XML = "<test></test>";
    String url = "/store/";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyRequested(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "/" }, in, out);
  }
  
  /**
   * Test if a layer can be exported
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void exportLayer(TestContext context) throws Exception {
    String XML = "<test></test>";
    String url = "/store/hello/world/";
    stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(XML)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyRequested(url, context);
      async.complete();
    });
    
    cmd.run(new String[] { "/hello/world" }, in, out);
  }
}
