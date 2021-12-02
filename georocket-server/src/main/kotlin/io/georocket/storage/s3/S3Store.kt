package io.georocket.storage.s3

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkMeta
import io.georocket.storage.IndexMeta
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.future.await
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.net.URI

/**
 * Stores chunks on Amazon S3
 * @author Michel Kraemer
 */
class S3Store(vertx: Vertx, accessKey: String? = null, secretKey: String? = null,
    endpoint: String? = null, bucket: String? = null, region: String? = null) : IndexedStore(vertx) {
  private val bucket: String
  private val s3: S3AsyncClient

  init {
    val config = vertx.orCreateContext.config()

    val actualAccessKey = accessKey ?: config.getString(ConfigConstants.STORAGE_S3_ACCESS_KEY) ?:
        throw IllegalArgumentException("Missing configuration item \"" +
            ConfigConstants.STORAGE_S3_ACCESS_KEY + "\"")

    val actualSecretKey = secretKey ?: config.getString(ConfigConstants.STORAGE_S3_SECRET_KEY) ?:
        throw IllegalArgumentException("Missing configuration item \"" +
            ConfigConstants.STORAGE_S3_SECRET_KEY + "\"")

    val actualEndpoint = endpoint ?: config.getString(ConfigConstants.STORAGE_S3_ENDPOINT) ?:
        throw IllegalArgumentException("Missing configuration item \"" +
            ConfigConstants.STORAGE_S3_ENDPOINT + "\"")

    this.bucket = bucket ?: config.getString(ConfigConstants.STORAGE_S3_BUCKET) ?:
        throw IllegalArgumentException("Missing configuration item \"" +
            ConfigConstants.STORAGE_S3_BUCKET + "\"")

    val actualRegion = region ?: config.getString(ConfigConstants.STORAGE_S3_REGION)

    val pathStyleAccess = config.getBoolean(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, true)

    var s3Builder = S3AsyncClient.builder()
        .endpointOverride(URI(actualEndpoint))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(actualAccessKey, actualSecretKey)))
        .serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(pathStyleAccess)
            .build())

    if (actualRegion != null) {
      s3Builder = s3Builder.region(Region.of(actualRegion))
    }

    s3 = s3Builder.build()
  }

  override suspend fun add(chunk: Buffer, chunkMetadata: ChunkMeta,
      indexMetadata: IndexMeta, layer: String): String {
    val path = layer.ifEmpty { "/" }

    // generate new file name
    val id = indexMetadata.correlationId + UniqueID.next()
    val filename = PathUtils.join(path, id)
    val key = PathUtils.removeLeadingSlash(filename)

    val objectRequest = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

    s3.putObject(objectRequest, AsyncRequestBody.fromBytes(chunk.byteBuf.array())).await()

    return filename
  }

  override suspend fun getOne(path: String): Buffer {
    val key = PathUtils.removeLeadingSlash(PathUtils.normalize(path))

    val getObjectRequest = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

    val response = s3.getObject(getObjectRequest, AsyncResponseTransformer.toBytes()).await()
    return Buffer.buffer(response.asByteArrayUnsafe())
  }

  override suspend fun doDeleteChunks(paths: Iterable<String>) {
    // only delete 1000 chunks in one request (this is the maximum number
    // specified by the S3 API)
    val windows = paths.windowed(1000, 1000, true)
    for (window in windows) {
      val identifiers = window.map { ObjectIdentifier.builder().key(it).build() }
      val deleteObjectsRequest = DeleteObjectsRequest.builder()
          .bucket(bucket)
          .delete(Delete.builder()
              .objects(identifiers)
              .build())
          .build()
      s3.deleteObjects(deleteObjectsRequest).await()
    }
  }
}
