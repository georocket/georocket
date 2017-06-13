package io.georocket.commands;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

/**
 * Test for {@link GetPropertyCommand}
 * @author Tim Hellhake
 */
@RunWith(VertxUnitRunner.class)
public class GetPropertyCommandTest extends CommandTestBase<GetPropertyCommand> {
  @Override
  protected GetPropertyCommand createCommand() {
    return new GetPropertyCommand();
  }

  /**
   * Test a simple get
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void testGetProperty(TestContext context) throws Exception {
    String body = "a,b,c";
    String url = "/store/?property=test";
    stubFor(get(urlEqualTo(url))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody(body)));

    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      context.assertEquals(body, writer.toString());
      verifyRequested(url, context);
      async.complete();
    });

    cmd.run(new String[]{"-l", "/", "-prop", "test"}, in, out);
  }
}
