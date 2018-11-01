package io.georocket.storage.s3;

import java.net.URL;
import java.util.Date;
import java.util.Queue;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.util.AwsHostNameUtils;
import com.google.common.base.Preconditions;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Stores chunks on Amazon S3
 * @author Michel Kraemer
 */
public class S3Store extends IndexedStore {
  private static Logger log = LoggerFactory.getLogger(S3Store.class);

  private final Vertx vertx;
  private AmazonS3 s3Client;
  private final String accessKey;
  private final String secretKey;
  private final String host;
  private final int port;
  private final String bucket;
  private final boolean pathStyleAccess;
  private final boolean forceSignatureV2;
  private final int requestExpirySeconds;
  private final HttpClient client;

  /**
   * Constructs a new store
   * @param vertx the Vert.x instance
   */
  public S3Store(Vertx vertx) {
    super(vertx);
    this.vertx = vertx;

    JsonObject config = vertx.getOrCreateContext().config();

    accessKey = config.getString(ConfigConstants.STORAGE_S3_ACCESS_KEY);
    Preconditions.checkNotNull(accessKey, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_ACCESS_KEY + "\"");

    secretKey = config.getString(ConfigConstants.STORAGE_S3_SECRET_KEY);
    Preconditions.checkNotNull(secretKey, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_SECRET_KEY + "\"");

    host = config.getString(ConfigConstants.STORAGE_S3_HOST);
    Preconditions.checkNotNull(host, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_HOST + "\"");

    port = config.getInteger(ConfigConstants.STORAGE_S3_PORT, 80);

    bucket = config.getString(ConfigConstants.STORAGE_S3_BUCKET);
    Preconditions.checkNotNull(bucket, "Missing configuration item \"" +
        ConfigConstants.STORAGE_S3_BUCKET + "\"");

    pathStyleAccess = config.getBoolean(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, true);
    forceSignatureV2 = config.getBoolean(ConfigConstants.STORAGE_S3_FORCE_SIGNATURE_V2, false);
    requestExpirySeconds = config.getInteger(ConfigConstants.STORAGE_S3_REQUEST_EXPIRY_SECONDS, 600);

    HttpClientOptions options = new HttpClientOptions();
    options.setDefaultHost(host);
    options.setDefaultPort(port);
    client = vertx.createHttpClient(options);
  }

  /**
   * Get or initialize the S3 client.
   * Note: this method must be synchronized because we're accessing the
   * {@link #s3Client} field and we're calling this method from a worker thread.
   * @return the S3 client
   */
  private synchronized AmazonS3 getS3Client() {
    if (s3Client == null) {
      BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

      AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials));

      if (forceSignatureV2) {
        ClientConfigurationFactory configFactory = new ClientConfigurationFactory();
        ClientConfiguration config = configFactory.getConfig();
        config.setSignerOverride("S3SignerType");
        builder = builder.withClientConfiguration(config);
      }

      String endpoint = "http://" + host + ":" + port;
      String clientRegion = null;
      if (!ServiceUtils.isS3USStandardEndpoint(endpoint)) {
        clientRegion = AwsHostNameUtils.parseRegion(host,
            AmazonS3Client.S3_SERVICE_NAME);
      }

      builder = builder.withEndpointConfiguration(new EndpointConfiguration(
          endpoint, clientRegion));
      builder = builder.withPathStyleAccessEnabled(pathStyleAccess);

      s3Client = builder.build();
    }
    return s3Client;
  }

  /**
   * Generate a pre-signed URL that can be used to make an HTTP request.
   * Note: this method must be synchronized because we're accessing the
   * {@link #s3Client} field and we're calling this method from a worker thread.
   * @param key the key of the S3 object to query
   * @param method the HTTP method that will be used in the request
   * @return the presigned URL
   */
  private synchronized URL generatePresignedUrl(String key, HttpMethod method) {
    Date expiry = new Date(System.currentTimeMillis() + 1000 * requestExpirySeconds);
    return getS3Client().generatePresignedUrl(bucket, key, expiry, method);
  }

  @Override
  protected void doAddChunk(String chunk, String path, String correlationId,
      Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }

    // generate new file name
    String id = generateChunkId(correlationId);
    String filename = PathUtils.join(path, id);
    String key = PathUtils.removeLeadingSlash(filename);

    vertx.<URL>executeBlocking(f -> {
      f.complete(generatePresignedUrl(key, HttpMethod.PUT));
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }

      URL u = ar.result();
      log.debug("PUT " + u);

      Buffer chunkBuf = Buffer.buffer(chunk);
      HttpClientRequest request = client.put(u.getFile());

      request.putHeader("Host", u.getHost());
      request.putHeader("Content-Length", String.valueOf(chunkBuf.length()));

      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });

      request.handler(response -> {
        Buffer errorBody = Buffer.buffer();
        if (response.statusCode() != 200) {
          response.handler(errorBody::appendBuffer);
        }
        response.endHandler(v -> {
          if (response.statusCode() == 200) {
            handler.handle(Future.succeededFuture(filename));
          } else {
            log.error(errorBody);
            handler.handle(Future.failedFuture(response.statusMessage()));
          }
        });
      });

      request.end(chunkBuf);
    });
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    String key = PathUtils.removeLeadingSlash(PathUtils.normalize(path));
    vertx.<URL>executeBlocking(f -> {
      f.complete(generatePresignedUrl(key, HttpMethod.GET));
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }

      URL u = ar.result();
      log.debug("GET " + u);

      HttpClientRequest request = client.get(ar.result().getFile());
      request.putHeader("Host", u.getHost());

      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });

      request.handler(response -> {
        if (response.statusCode() == 200) {
          String contentLength = response.getHeader("Content-Length");
          long chunkSize = Long.parseLong(contentLength);
          handler.handle(Future.succeededFuture(new DelegateChunkReadStream(chunkSize, response)));
        } else {
          Buffer errorBody = Buffer.buffer();
          response.handler(errorBody::appendBuffer);
          response.endHandler(v -> {
            log.error(errorBody);
            handler.handle(Future.failedFuture(response.statusMessage()));
          });
        }
      });

      request.end();
    });
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }

    String key = PathUtils.removeLeadingSlash(PathUtils.normalize(paths.poll()));
    vertx.<URL>executeBlocking(f -> {
      f.complete(generatePresignedUrl(key, HttpMethod.DELETE));
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }

      URL u = ar.result();
      log.debug("DELETE " + u);

      HttpClientRequest request = client.delete(ar.result().getFile());
      request.putHeader("Host", u.getHost());

      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });

      request.handler(response -> {
        Buffer errorBody = Buffer.buffer();
        if (response.statusCode() != 204) {
          response.handler(errorBody::appendBuffer);
        }
        response.endHandler(v -> {
          switch (response.statusCode()) {
            case 204:
            case 404:
              doDeleteChunks(paths, handler);
              break;
            default:
              log.error(errorBody);
              handler.handle(Future.failedFuture(response.statusMessage()));
          }
        });
      });

      request.end();
    });
  }
}
