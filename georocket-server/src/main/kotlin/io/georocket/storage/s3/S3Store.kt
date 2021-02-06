package io.georocket.storage.s3

import com.google.common.base.Preconditions
import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.georocket.util.UniqueID
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.future.await
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
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
class S3Store(vertx: Vertx) : IndexedStore(vertx) {
  private val bucket: String
  private val s3: S3AsyncClient

  init {
    val config = vertx.orCreateContext.config()

    val accessKey = config.getString(ConfigConstants.STORAGE_S3_ACCESS_KEY)
    Preconditions.checkNotNull(accessKey, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_ACCESS_KEY + "\"")

    val secretKey = config.getString(ConfigConstants.STORAGE_S3_SECRET_KEY)
    Preconditions.checkNotNull(secretKey, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_SECRET_KEY + "\"")

    val host = config.getString(ConfigConstants.STORAGE_S3_HOST)
    Preconditions.checkNotNull(host, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_HOST + "\"")

    val port = config.getInteger(ConfigConstants.STORAGE_S3_PORT, 80)

    bucket = config.getString(ConfigConstants.STORAGE_S3_BUCKET)
    Preconditions.checkNotNull(bucket, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_BUCKET + "\"")

    val pathStyleAccess = config.getBoolean(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, true)

    s3 = S3AsyncClient.builder()
        .endpointOverride(URI("s3://$host:$port"))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey)))
        .serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(pathStyleAccess)
            .build())
        .build()
  }

  override suspend fun doAddChunk(chunk: String, layer: String, correlationId: String): String {
    val path = if (layer.isEmpty()) "/" else layer

    // generate new file name
    val id = correlationId + UniqueID.next()
    val filename = PathUtils.join(path, id)
    val key = PathUtils.removeLeadingSlash(filename)

    val objectRequest = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

    s3.putObject(objectRequest, AsyncRequestBody.fromString(chunk)).await()

    return filename
  }

  override suspend fun getOne(path: String): ChunkReadStream {
    val key = PathUtils.removeLeadingSlash(PathUtils.normalize(path))

    val getObjectRequest = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

    val response = s3.getObject(getObjectRequest, AsyncResponseTransformer.toBytes()).await()
    val chunk = Buffer.buffer(response.asByteArrayUnsafe())

    return DelegateChunkReadStream(chunk)
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
