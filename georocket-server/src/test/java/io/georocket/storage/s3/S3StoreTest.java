package io.georocket.storage.s3;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

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

  private static final class Http {
    public static final class Types {
      public static final String XML  = "application/xml";
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

  /**
   * The http mock test rule
   */
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(S3_PORT);

  private static String pathWithLeadingSlash(String... paths) {
    return "/" + PathUtils.join(paths);
  }

  /**
   * Set up test dependencies.
   */
  @Before
  public void setUp() {
    wireMockRule.start();

    // Mock http request for getOne
    wireMockRule.stubFor(
        // Request
        get(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, ID, "*")))
            .willReturn(aResponse()
                .withStatus(Http.Codes.OK)

                .withHeader(Http.CONTENT_LENGTH, String.valueOf(CHUNK_CONTENT.length()))
                .withHeader(Http.CONTENT_TYPE,   Http.Types.XML)
                .withHeader(Http.CONNECTION,     "close")
                .withHeader(Http.SERVER,         "AmazonS3")

                .withBody(CHUNK_CONTENT)
            )
    );

    // Mock http request for add without tempFolder
    wireMockRule.stubFor(
        put(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, "*")))
            .withHeader(Http.CONTENT_LENGTH, equalTo(String.valueOf(CHUNK_CONTENT.length())))

            .withRequestBody(equalTo(CHUNK_CONTENT))

            .willReturn(aResponse()
                .withStatus(Http.Codes.OK)
            )
    );

    wireMockRule.stubFor(
        put(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, TEST_FOLDER, "*")))
            .withHeader(Http.CONTENT_LENGTH, equalTo(String.valueOf(CHUNK_CONTENT.length())))

            .withRequestBody(equalTo(CHUNK_CONTENT))

            .willReturn(aResponse()
                .withStatus(Http.Codes.OK)
            )
    );

    String listItems = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
        "  <Name>quotes</Name>\n" +
        "  <Prefix>E</Prefix>\n" +
        "  <StartAfter>ExampleGuide.pdf</StartAfter>\n" +
        "  <KeyCount>3</KeyCount>\n" +
        "  <MaxKeys>3</MaxKeys>\n" +
        "  <IsTruncated>false</IsTruncated>\n" +
        "  <Contents>\n" +
        "    <Key>ExampleObject1.txt</Key>\n" +
        "    <LastModified>2013-09-17T18:07:53.000Z</LastModified>\n" +
        "    <ETag>&quot;599bab3ed2c697f1d26842727561fd94&quot;</ETag>\n" +
        "    <Size>857</Size>\n" +
        "    <StorageClass>REDUCED_REDUNDANCY</StorageClass>\n" +
        "  </Contents>\n" +
        "  <Contents>\n" +
        "    <Key>ExampleObject2.txt</Key>\n" +
        "    <LastModified>2013-09-17T18:07:53.000Z</LastModified>\n" +
        "    <ETag>&quot;599bab3ed2c697f1d26842727561fd20&quot;</ETag>\n" +
        "    <Size>233</Size>\n" +
        "    <StorageClass>REDUCED_REDUNDANCY</StorageClass>\n" +
        "  </Contents>\n" +
        "  <Contents>\n" +
        "    <Key>ExampleObject3.txt</Key>\n" +
        "    <LastModified>2013-09-17T18:07:53.000Z</LastModified>\n" +
        "    <ETag>&quot;599bab3ed2c697f1d26842727561fd30&quot;</ETag>\n" +
        "    <Size>412</Size>\n" +
        "    <StorageClass>REDUCED_REDUNDANCY</StorageClass>\n" +
        "  </Contents>\n" +
        "</ListBucketResult>";

    wireMockRule.stubFor(
        get(urlMatching(pathWithLeadingSlash(S3_BUCKET) + "/\\?list-type=2.*"))

        .willReturn(aResponse()
            .withStatus(Http.Codes.OK)
            .withHeader("Content-Type", "application/xml")
            .withHeader("Content-Length", String.valueOf(listItems.length()))

            .withBody(listItems)
        )
    );

    // Mock http request for delete without tempFolder
    wireMockRule.stubFor(
        delete(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, ID, "*")))

        .willReturn(aResponse()
            .withStatus(Http.Codes.NO_CONTENT)
        )
    );

    // Mock http request for delete with tempFolder
    wireMockRule.stubFor(
        delete(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, TEST_FOLDER, ID, "*")))

        .willReturn(aResponse()
            .withStatus(Http.Codes.NO_CONTENT)
        )
    );
  }

  /**
   * Stop WireMock
   */
  @After
  public void tearDown() {
    wireMockRule.stop();
  }

  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    config.put(ConfigConstants.STORAGE_S3_ACCESS_KEY,        S3_ACCESS_KEY);
    config.put(ConfigConstants.STORAGE_S3_SECRET_KEY,        S3_SECRET_KEY);
    config.put(ConfigConstants.STORAGE_S3_HOST,              S3_HOST);
    config.put(ConfigConstants.STORAGE_S3_PORT,              S3_PORT);
    config.put(ConfigConstants.STORAGE_S3_BUCKET,            S3_BUCKET);
    config.put(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, S3_PATH_STYLE_ACCESS);
  }

  @Override
  protected Store createStore(Vertx vertx) {
    configureVertx(vertx);
    return new S3Store(vertx);
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<String>> handler) {
    handler.handle(Future.succeededFuture(PathUtils.join(path, ID)));
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    verify(putRequestedFor(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, path, "*"))));
    handler.handle(Future.succeededFuture());
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    verify(deleteRequestedFor(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, path, "*"))));
    handler.handle(Future.succeededFuture());
  }
}
