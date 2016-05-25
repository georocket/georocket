package io.georocket.storage.s3;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import org.bson.types.ObjectId;

import java.net.URL;
import java.util.Queue;

/**
 * Stores chunks on Amazon S3
 * @author Michel Kraemer
 */
public class S3Store extends IndexedStore {
  private static Logger log = LoggerFactory.getLogger(S3Store.class);

  private final Vertx vertx;
  private AmazonS3Client s3Client;
  private final String accessKey;
  private final String secretKey;
  private final String host;
  private final int port;
  private final String bucket;
  private final boolean pathStyleAccess;
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
    secretKey = config.getString(ConfigConstants.STORAGE_S3_SECRET_KEY);
    host = config.getString(ConfigConstants.STORAGE_S3_HOST);
    port = config.getInteger(ConfigConstants.STORAGE_S3_PORT, 80);
    bucket = config.getString(ConfigConstants.STORAGE_S3_BUCKET);
    pathStyleAccess = config.getBoolean(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, true);

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
  private synchronized AmazonS3Client getS3Client() {
    if (s3Client == null) {
      BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      s3Client = new AmazonS3Client(credentials);
      s3Client.setEndpoint("http://" + host + ":" + port);
      s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
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
    return getS3Client().generatePresignedUrl(bucket, key, null, method);
  }

  @Override
  protected void doAddChunk(String chunk, String path, Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }

    // generate new file name
    String id = new ObjectId().toString();
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
          response.handler(buf -> {
            errorBody.appendBuffer(buf);
          });
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
          response.handler(buf -> {
            errorBody.appendBuffer(buf);
          });
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
  public void getStoredSize(Handler<AsyncResult<Long>> handler) {
    vertx.<Long>executeBlocking(f -> {
      // http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingObjectKeysUsingJava.html
      ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket);
      ListObjectsV2Result result;
      Long size = 0L;

      do {
        result = getS3Client().listObjectsV2(req);

        for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
          size = size + objectSummary.getSize();
        }

        req.setContinuationToken(result.getNextContinuationToken());
      } while (result.isTruncated());

      f.complete(size);
    }, h -> {
      if (h.failed()) {
        log.fatal("Could not calculate the used size of amazon S3 bucket.");
        handler.handle(Future.failedFuture(h.cause()));
      } else {
        handler.handle(Future.succeededFuture(h.result()));
      }
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
          response.handler(buf -> {
            errorBody.appendBuffer(buf);
          });
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
