package io.georocket.commands;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.github.tomakehurst.wiremock.client.VerificationException;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test for {@link ImportCommand}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class ImportCommandTest extends CommandTestBase<ImportCommand> {
  /**
   * A temporary folder for test files
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  /**
   * Test XML file contents to import
   */
  private static final String XML = "<test></test>";
  
  /**
   * The XML file to import
   */
  private File testFile;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    testFile = folder.newFile("test");
    FileUtils.write(testFile, XML, StandardCharsets.UTF_8);
  }
  
  @Override
  protected ImportCommand createCommand() {
    return new ImportCommand();
  }
  
  /**
   * Test no file pattern
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void noFilePattern(TestContext context) throws Exception {
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(1, exitCode);
      async.complete();
    });
    context.assertEquals(1, cmd.run(new String[] { }, in, out));
  }
  
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
   * @throws Exception if something goes wrong
   */
  @Test
  public void simpleImport(TestContext context) throws Exception {
    String url = "/store";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyPosted(url, XML, context);
      async.complete();
    });
    
    cmd.run(new String[] { testFile.getAbsolutePath() }, in, out);
  }
  
  /**
   * Test importing to a layer
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void importLayer(TestContext context) throws Exception {
    String url = "/store/hello/world/";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyPosted(url, XML, context);
      async.complete();
    });
    
    cmd.run(new String[] { "-l", "hello/world", testFile.getAbsolutePath() }, in, out);
  }
  
  /**
   * Test importing with tags
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void importTags(TestContext context) throws Exception {
    String url = "/store?tags=hello%2Cworld";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));
    
    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyPosted(url, XML, context);
      async.complete();
    });
    
    cmd.run(new String[] { "-t", "hello,world", testFile.getAbsolutePath() }, in, out);
  }

  /**
   * Test importing with properties
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void importProperties(TestContext context) throws Exception {
    String url = "/store?props=hello%3Aworld%2CmyKey%3AmyValue";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));

    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyPosted(url, XML, context);
      async.complete();
    });

    cmd.run(new String[] { "-props", "hello:world,myKey:myValue", testFile.getAbsolutePath() }, in, out);
  }

  /**
   * Test importing with properties including an escaped character
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void importPropertiesEscaping(TestContext context) throws Exception {
    String url = "/store?props=hello%3Aworld%2CmyKey%3Amy%5C%3AValue%2Cmy%5C%3AKey%3AmyValue";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));

    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyPosted(url, XML, context);
      async.complete();
    });

    cmd.run(new String[] { "-props", "hello:world,myKey:my\\:Value,my\\:Key:myValue", testFile.getAbsolutePath() }, in, out);
  }

  /**
   * Test importing with tags
   * @param context the test context
   * @throws Exception if something goes wrong
   */
  @Test
  public void importFallbackCRS(TestContext context) throws Exception {
    String url = "/store?fallbackCRS=test";
    stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(202)));

    Async async = context.async();
    cmd.setEndHandler(exitCode -> {
      context.assertEquals(0, exitCode);
      verifyPosted(url, XML, context);
      async.complete();
    });

    cmd.run(new String[] { "-c", "test", testFile.getAbsolutePath() }, in, out);
  }
}
