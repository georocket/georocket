package io.georocket.storage.s3

import io.georocket.coVerify
import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.net.URI
import java.nio.charset.StandardCharsets

/**
 * Test [S3Store]
 * @author Michel Kraemer
 */
@Testcontainers
class S3StoreTest : StorageTest() {
  companion object {
    private const val ACCESS_KEY = "minioadmin"
    private const val SECRET_KEY = "minioadmin"
    private const val BUCKET = "georocket"
  }

  private lateinit var endpoint: String
  private lateinit var s3: S3AsyncClient

  @Container
  val s3server: GenericContainer<Nothing> = GenericContainer<Nothing>("minio/minio").apply {
    withExposedPorts(9000)
    withCommand("server", "/data")
  }

  @BeforeEach
  fun setUp(ctx: VertxTestContext, vertx: Vertx) {
    endpoint = "http://${s3server.host}:${s3server.firstMappedPort}"

    // create bucket
    s3 = S3AsyncClient.builder()
        .endpointOverride(URI(endpoint))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .build()
    GlobalScope.launch(vertx.dispatcher()) {
      s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).await()
      ctx.completeNow()
    }
  }

  @AfterEach
  fun tearDown() {
    s3.close()
  }

  override suspend fun createStore(vertx: Vertx): Store {
    return S3Store(vertx, ACCESS_KEY, SECRET_KEY, endpoint, BUCKET)
  }

  override suspend fun prepareData(ctx: VertxTestContext, vertx: Vertx, path: String?): String {
    val key = PathUtils.join(path, ID)
    val objectRequest = PutObjectRequest.builder()
        .bucket(BUCKET)
        .key(key)
        .build()
    s3.putObject(objectRequest, AsyncRequestBody.fromString(CHUNK_CONTENT)).await()
    return key
  }

  override suspend fun validateAfterStoreAdd(ctx: VertxTestContext, vertx: Vertx, path: String?) {
    val objects = s3.listObjects(ListObjectsRequest.builder().bucket(BUCKET).build()).await()
    val keys = objects.contents().map { it.key() }
    ctx.coVerify {
      assertThat(keys).hasSize(1)

      val getObjectRequest = GetObjectRequest.builder()
          .bucket(BUCKET)
          .key(keys[0])
          .build()
      val response = s3.getObject(getObjectRequest, AsyncResponseTransformer.toBytes()).await()
      val chunk = response.asString(StandardCharsets.UTF_8)
      assertThat(chunk).isEqualTo(CHUNK_CONTENT)
    }
  }

  override suspend fun validateAfterStoreDelete(ctx: VertxTestContext, vertx: Vertx, path: String) {
    val objects = s3.listObjects(ListObjectsRequest.builder().bucket(BUCKET).build()).await()
    ctx.verify {
      assertThat(objects.contents()).isEmpty()
    }
  }
}
