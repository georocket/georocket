package de.fhg.igd.georocket;

import static de.fhg.igd.georocket.constants.ConfigConstants.DEFAULT_PORT;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import de.fhg.igd.georocket.constants.ConfigConstants;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pump;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Integration tests for GeoRocket
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class GeoRocketTest {
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
   * Tests if a small CityGML file can be uploaded correctly
   * @param context the test context
   * @throws Exception if an exception occurs
   */
  @Test
  public void testMiniFile(TestContext context) throws Exception {
    String testFile = "berlin_alexanderplatz_mini.xml";
    
    Vertx vertx = rule.vertx();
    Async async = context.async();
    
    // set GeoRocket home to temporary folder
    JsonObject config = new JsonObject();
    config.put(ConfigConstants.HOME, folder.getRoot().getAbsolutePath());
    DeploymentOptions options = new DeploymentOptions();
    options.setConfig(config);
    
    // deploy GeoRocket
    vertx.deployVerticle(GeoRocket.class.getName(), options, context.asyncAssertSuccess(id -> {
      // load test file
      FileSystem fs = vertx.fileSystem();
      OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
      fs.props(testFile, context.asyncAssertSuccess(props -> {
        fs.open(testFile, openOptions, context.asyncAssertSuccess(f -> {
          // send HTTP request (i.e. upload file)
          HttpClientOptions clientOptions = new HttpClientOptions()
              .setTryUseCompression(true);
          HttpClient client = vertx.createHttpClient(clientOptions);
          HttpClientRequest request = client.post(DEFAULT_PORT, "localhost", "/db", response -> {
            // check response and contents of GeoRocket's storage folder
            vertx.setTimer(100, l -> {
              context.assertEquals(202, response.statusCode());
              context.assertEquals(2, new File(folder.getRoot(), "storage/file").listFiles().length);
              async.complete();
            });
          });
          request.putHeader("Content-Length", String.valueOf(props.size()));
          Pump.pump(f, request).start();
          f.endHandler(v -> request.end());
        }));
      }));
    }));
  }
}
