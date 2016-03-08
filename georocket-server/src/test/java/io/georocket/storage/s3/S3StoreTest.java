package io.georocket.storage.s3;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Test {@link S3Store}
 * @author Andrej Sajenko
 */
public class S3StoreTest extends StorageTest {

  private static String  S3_ACCESS_KEY        = "640ab2bae07bedc4c163f679a746f7ab7fb5d1fa";
  private static String  S3_SECRET_KEY        = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3";
  private static String  S3_HOST              = "localhost";
  private static int     S3_PORT              = 8080;
  private static String  S3_BUCKET            = "testbucket";
  private static boolean S3_PATH_STYLE_ACCESS = true;

  private static final class HTTP {
    public static final class Types {
      public static final String XML  = "application/xml";
      public static final String TEXT = "text/plain";
    }
    public static final class Codes {
      public static final int OK = 200;
      public static final int NO_CONTENT = 204;
    }

    public static final String CONTENT_TYPE   = "Content-Type";
    public static final String SERVER         = "Server";
    public static final String CONNECTION     = "Connection";
    public static final String CONTENT_LENGTH = "Content-Length";
  }

  private static String HTTP_REQUEST_HOST_FOR_BUCKED = String.format("%s.s3.amazonaws.com", S3_BUCKET);

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(S3_PORT);



  private static final String pathWithoutFolder = String.format("/%s/%s", S3_BUCKET, StorageTest.id);
  private static final String pathWithFolder = String.format("/%s/%s/%s", S3_BUCKET, StorageTest.testFolder, StorageTest.id);

  private static final String uriPatternWithoutFolder = pathWithoutFolder + "/*";
  private static final String uriPatternWithFolder = pathWithFolder + "/*";



  @Before
  public void setUp() {
    wireMockRule.start();

    // Mock http request for getOne
    wireMockRule.stubFor(
        // Request
        get(urlPathMatching(uriPatternWithoutFolder))
            .willReturn(aResponse()
                .withStatus(HTTP.Codes.OK)

                .withHeader(HTTP.CONTENT_LENGTH,    String.valueOf(StorageTest.chunkContent.length()))
                .withHeader(HTTP.CONTENT_TYPE,      HTTP.Types.XML)
                .withHeader(HTTP.CONNECTION,        "close")
                .withHeader(HTTP.SERVER,            "AmazonS3")

                .withBody(StorageTest.chunkContent)
            )
    );

    // Mock http request for add without folder
    wireMockRule.stubFor(
        put(urlPathMatching(String.format("/%s/*", S3_BUCKET)))
            .withHeader(HTTP.CONTENT_LENGTH,    equalTo(String.valueOf(StorageTest.chunkContent.length())))

            .withRequestBody(equalTo(StorageTest.chunkContent))

            .willReturn(aResponse()
                .withStatus(HTTP.Codes.OK)
            )
    );

    wireMockRule.stubFor(
        put(urlPathMatching(String.format("/%s/%s/*", S3_BUCKET, StorageTest.testFolder)))
            .withHeader(HTTP.CONTENT_LENGTH,    equalTo(String.valueOf(StorageTest.chunkContent.length())))

            .withRequestBody(equalTo(StorageTest.chunkContent))

            .willReturn(aResponse()
                .withStatus(HTTP.Codes.OK)
            )
    );

    // Mock http request for delete without folder
    wireMockRule.stubFor(
        delete(urlPathMatching(uriPatternWithoutFolder))

        .willReturn(aResponse()
            .withStatus(HTTP.Codes.NO_CONTENT)
        )
    );

    // Mock http request for delete with folder
    wireMockRule.stubFor(
        delete(urlPathMatching(uriPatternWithFolder))

            .willReturn(aResponse()
                .withStatus(HTTP.Codes.NO_CONTENT)
            )
    );
  }

  @After
  public void tearDown() {
    wireMockRule.stop();
  }

  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    config.put(ConfigConstants.STORAGE_S3_ACCESS_KEY, S3_ACCESS_KEY);
    config.put(ConfigConstants.STORAGE_S3_SECRET_KEY, S3_SECRET_KEY);
    config.put(ConfigConstants.STORAGE_S3_HOST, S3_HOST);
    config.put(ConfigConstants.STORAGE_S3_PORT, S3_PORT);
    config.put(ConfigConstants.STORAGE_S3_BUCKET, S3_BUCKET);
    config.put(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, S3_PATH_STYLE_ACCESS);
  }

  @Override
  protected Store createStore(Vertx vertx) {
    this.configureVertx(vertx);
    return new S3Store(vertx);
  }

  @Override
  protected Handler<Future<String>> prepare_Data(TestContext context, Vertx vertx, String path) {
    return h -> h.complete(PathUtils.join(path, id));
  }

  @Override
  protected Handler<Future<Object>> validate_after_Store_add(TestContext context, Vertx vertx, String path) {
    return h -> {
      verify(putRequestedFor(urlPathMatching(path == null || path.isEmpty() ? String.format("/%s/*", S3_BUCKET) : String.format("/%s/%s/*", S3_BUCKET, StorageTest.testFolder))));

      h.complete();
    };
  }

  @Override
  protected Handler<Future<Object>> validate_after_Store_delete(TestContext context, Vertx vertx, String path) {
    return h -> {
      verify(deleteRequestedFor(urlPathMatching(path == null || path.isEmpty() ? uriPatternWithoutFolder : uriPatternWithFolder)));

      h.complete();
    };
  }
}
