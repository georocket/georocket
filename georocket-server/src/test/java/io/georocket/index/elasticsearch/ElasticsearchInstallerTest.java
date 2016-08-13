package io.georocket.index.elasticsearch;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link ElasticsearchInstaller}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class ElasticsearchInstallerTest {
  private final String ZIP_NAME = "elasticsearch-dummy.zip";
  
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  /**
   * Create a temporary folder
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  /**
   * Run a mock HTTP server
   */
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());
  
  /**
   * The URL pointing to the elasticsearch dummy zip file
   */
  private String downloadUrl;
  
  /**
   * Set up the unit tests
   */
  @Before
  public void setUp() {
    configureFor("localhost", wireMockRule.port());
    downloadUrl = "http://localhost:" + wireMockRule.port() + "/" + ZIP_NAME;
  }
  
  /**
   * Test if Elasticsearch is downloaded correctly
   * @param context the test context
   * @throws IOException if the temporary folder for download could not be created
   */
  @Test
  public void download(TestContext context) throws IOException {
    URL zipFile = this.getClass().getResource(ZIP_NAME);
    byte[] zipFileContents = IOUtils.toByteArray(zipFile);
    
    stubFor(get(urlEqualTo("/" + ZIP_NAME))
        .willReturn(aResponse()
            .withBody(zipFileContents)));
    
    Vertx vertx = rule.vertx();
    Async async = context.async();
    File dest = folder.newFolder();
    dest.delete();
    new ElasticsearchInstaller(new io.vertx.rxjava.core.Vertx(vertx))
      .download(downloadUrl, dest.getAbsolutePath())
      .subscribe(path -> {
        verify(getRequestedFor(urlEqualTo("/" + ZIP_NAME)));
        context.assertEquals(dest.getAbsolutePath(), path);
        context.assertTrue(dest.exists());
        context.assertTrue(new File(dest, "bin/elasticsearch").exists());
        if (!SystemUtils.IS_OS_WINDOWS) {
          context.assertTrue(new File(dest, "bin/elasticsearch").canExecute());
        }
        async.complete();
      }, context::fail);
  }
  
  /**
   * Test if a server error is handled correctly
   * @param context the test context
   * @throws IOException if the temporary folder for download could not be created
   */
  @Test
  public void download500(TestContext context) throws IOException {
    stubFor(get(urlEqualTo("/" + ZIP_NAME))
        .willReturn(aResponse()
            .withStatus(500)));
    
    Vertx vertx = rule.vertx();
    Async async = context.async();
    File dest = folder.newFolder();
    dest.delete();
    new ElasticsearchInstaller(new io.vertx.rxjava.core.Vertx(vertx))
      .download(downloadUrl, dest.getAbsolutePath())
      .subscribe(path -> context.fail("Download is expected to fail"),
          err -> {
            verify(getRequestedFor(urlEqualTo("/" + ZIP_NAME)));
            context.assertFalse(dest.exists());
            async.complete();
          });
  }
  
  /**
   * Test if the installer fails if the returned archive is invalid
   * @param context the test context
   * @throws IOException if the temporary folder for download could not be created
   */
  @Test
  public void downloadInvalidArchive(TestContext context) throws IOException {
    stubFor(get(urlEqualTo("/" + ZIP_NAME))
        .willReturn(aResponse()
            .withBody("foobar")));
    
    Vertx vertx = rule.vertx();
    Async async = context.async();
    File dest = folder.newFolder();
    dest.delete();
    new ElasticsearchInstaller(new io.vertx.rxjava.core.Vertx(vertx))
      .download(downloadUrl, dest.getAbsolutePath())
      .subscribe(path -> context.fail("Download is expected to fail"),
          err -> {
            verify(getRequestedFor(urlEqualTo("/" + ZIP_NAME)));
            context.assertFalse(dest.exists());
            async.complete();
          });
  }
  
  /**
   * Test if the installer fails if it receives random data and the stream is
   * closed in between
   * @param context the test context
   * @throws IOException if the temporary folder for download could not be created
   */
  @Test
  public void downloadRandomAndClose(TestContext context) throws IOException {
    stubFor(get(urlEqualTo("/" + ZIP_NAME))
        .willReturn(aResponse()
            .withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
    
    Vertx vertx = rule.vertx();
    Async async = context.async();
    File dest = folder.newFolder();
    dest.delete();
    new ElasticsearchInstaller(new io.vertx.rxjava.core.Vertx(vertx))
      .download(downloadUrl, dest.getAbsolutePath())
      .subscribe(path -> context.fail("Download is expected to fail"),
          err -> {
            verify(getRequestedFor(urlEqualTo("/" + ZIP_NAME)));
            context.assertFalse(dest.exists());
            async.complete();
          });
  }
}
