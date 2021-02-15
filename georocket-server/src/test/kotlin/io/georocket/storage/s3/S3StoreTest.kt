package io.georocket.storage.s3

import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.junit.WireMockRule
import io.georocket.constants.ConfigConstants
import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

/**
 * Test [S3Store]
 * @author Andrej Sajenko
 */
class S3StoreTest : StorageTest() {
  companion object {
    private const val S3_ACCESS_KEY = "640ab2bae07bedc4c163f679a746f7ab7fb5d1fa"
    private const val S3_SECRET_KEY = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
    private const val S3_HOST = "localhost"
    private const val S3_BUCKET = "testbucket"
    private const val S3_PATH_STYLE_ACCESS = true

    private fun pathWithLeadingSlash(vararg paths: String): String {
      return "/" + PathUtils.join(*paths)
    }
  }

  /**
   * Set up test dependencies.
   */
  @BeforeEach
  fun setUp() {
    wireMockRule.start()
    WireMock.configureFor("localhost", wireMockRule.port())

    // Mock http request for getOne
    wireMockRule.stubFor( // Request
        WireMock.get(WireMock.urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, ID)))
            .willReturn(WireMock.aResponse()
                .withStatus(Http.Codes.OK)
                .withHeader(Http.CONTENT_LENGTH, CHUNK_CONTENT.length.toString())
                .withHeader(Http.CONTENT_TYPE, Http.Types.XML)
                .withHeader(Http.CONNECTION, "close")
                .withHeader(Http.SERVER, "AmazonS3")
                .withBody(CHUNK_CONTENT)
            )
    )

    // Mock http request for add without tempFolder
    wireMockRule.stubFor(
        WireMock.put(WireMock.urlPathMatching(pathWithLeadingSlash(S3_BUCKET, ".*")))
            .withHeader(Http.CONTENT_LENGTH, WireMock.equalTo(CHUNK_CONTENT.length.toString()))
            .withRequestBody(WireMock.equalTo(CHUNK_CONTENT))
            .willReturn(WireMock.aResponse()
                .withStatus(Http.Codes.OK)
            )
    )
    wireMockRule.stubFor(
        WireMock.put(WireMock.urlPathMatching(pathWithLeadingSlash(S3_BUCKET, TEST_FOLDER, ".*")))
            .withHeader(Http.CONTENT_LENGTH, WireMock.equalTo(CHUNK_CONTENT.length.toString()))
            .withRequestBody(WireMock.equalTo(CHUNK_CONTENT))
            .willReturn(WireMock.aResponse()
                .withStatus(Http.Codes.OK)
            )
    )
    val listItems = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>$S3_BUCKET</Name>
  <KeyCount>1</KeyCount>
  <MaxKeys>3</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>ExampleObject1.txt</Key>
    <LastModified>2013-09-17T18:07:53.000Z</LastModified>
    <ETag>&quot;599bab3ed2c697f1d26842727561fd94&quot;</ETag>
    <Size>857</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>ExampleObject2.txt</Key>
    <LastModified>2013-09-17T18:07:53.000Z</LastModified>
    <ETag>&quot;599bab3ed2c697f1d26842727561fd20&quot;</ETag>
    <Size>233</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>ExampleObject3.txt</Key>
    <LastModified>2013-09-17T18:07:53.000Z</LastModified>
    <ETag>&quot;599bab3ed2c697f1d26842727561fd30&quot;</ETag>
    <Size>412</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"""
    wireMockRule.stubFor(
        WireMock.get(WireMock.urlMatching(pathWithLeadingSlash(S3_BUCKET) + "/\\?list-type=2.*"))
            .willReturn(WireMock.aResponse()
                .withStatus(Http.Codes.OK)
                .withHeader("Content-Type", "application/xml")
                .withHeader("Content-Length", listItems.length.toString())
                .withBody(listItems)
            )
    )

    // Mock http request for delete without tempFolder
    wireMockRule.stubFor(
        WireMock.delete(WireMock.urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, ID)))
            .willReturn(WireMock.aResponse()
                .withStatus(Http.Codes.NO_CONTENT)
            )
    )

    // Mock http request for delete with tempFolder
    wireMockRule.stubFor(
        WireMock.delete(WireMock.urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, TEST_FOLDER, ID)))
            .willReturn(WireMock.aResponse()
                .withStatus(Http.Codes.NO_CONTENT)
            )
    )
  }

  /**
   * Stop WireMock
   */
  @AfterEach
  fun tearDown() {
    wireMockRule.stop()
  }

  private fun configureVertx(vertx: Vertx) {
    val config = vertx.orCreateContext.config()
    config.put(ConfigConstants.STORAGE_S3_ACCESS_KEY, S3_ACCESS_KEY)
    config.put(ConfigConstants.STORAGE_S3_SECRET_KEY, S3_SECRET_KEY)
    config.put(ConfigConstants.STORAGE_S3_HOST, S3_HOST)
    config.put(ConfigConstants.STORAGE_S3_PORT, wireMockRule.port())
    config.put(ConfigConstants.STORAGE_S3_BUCKET, S3_BUCKET)
    config.put(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, S3_PATH_STYLE_ACCESS)
  }

  override fun createStore(vertx: Vertx): Store {
    configureVertx(vertx)
    return S3Store(vertx)
  }

  protected fun prepareData(context: TestContext?, vertx: Vertx?, path: String?,
      handler: Handler<AsyncResult<String?>?>) {
    handler.handle(Future.succeededFuture(PathUtils.join(path, ID)))
  }

  protected fun validateAfterStoreAdd(context: TestContext?, vertx: Vertx?,
      path: String?, handler: Handler<AsyncResult<Void?>?>) {
    WireMock.verify(WireMock.putRequestedFor(WireMock.urlPathMatching(pathWithLeadingSlash(S3_BUCKET, path!!, ".*"))))
    handler.handle(Future.succeededFuture())
  }

  protected fun validateAfterStoreDelete(context: TestContext?, vertx: Vertx?,
      path: String?, handler: Handler<AsyncResult<Void?>?>) {
    WireMock.verify(WireMock.deleteRequestedFor(WireMock.urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, path!!))))
    handler.handle(Future.succeededFuture())
  }
}
