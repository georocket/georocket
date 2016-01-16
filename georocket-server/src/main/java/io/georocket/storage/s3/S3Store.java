package io.georocket.storage.s3;

import java.net.URL;
import java.util.Queue;

import org.bson.types.ObjectId;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

import io.georocket.SimpleChunkReadStream;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;

/**
 * Stores chunks on Amazon S3
 * @author Michel Kraemer
 */
public class S3Store extends IndexedStore {
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
  
  private void ensureS3Client() {
    if (s3Client == null) {
      BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      s3Client = new AmazonS3Client(credentials);
      s3Client.setEndpoint("http://" + host + ":" + port);
      s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
    }
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
      ensureS3Client();
      URL u = s3Client.generatePresignedUrl(bucket, key, null, HttpMethod.PUT);
      f.complete(u);
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      HttpClientRequest request = client.put(ar.result().getFile());
      request.putHeader("Content-Length", String.valueOf(chunk.length()));
      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });
      request.handler(response -> {
        if (response.statusCode() == 200) {
          handler.handle(Future.succeededFuture(filename));
        } else {
          handler.handle(Future.failedFuture(response.statusMessage()));
        }
      });
      request.end(chunk);
    });
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    String key = PathUtils.removeLeadingSlash(PathUtils.normalize(path));
    vertx.<URL>executeBlocking(f -> {
      ensureS3Client();
      URL u = s3Client.generatePresignedUrl(bucket, key, null, HttpMethod.GET);
      f.complete(u);
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      HttpClientRequest request = client.get(ar.result().getFile());
      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });
      request.handler(response -> {
        if (response.statusCode() == 200) {
          String contentLength = response.getHeader("Content-Length");
          long chunkSize = Long.parseLong(contentLength);
          handler.handle(Future.succeededFuture(new SimpleChunkReadStream(chunkSize, response)));
        } else {
          handler.handle(Future.failedFuture(response.statusMessage()));
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
      ensureS3Client();
      URL u = s3Client.generatePresignedUrl(bucket, key, null, HttpMethod.DELETE);
      f.complete(u);
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      HttpClientRequest request = client.delete(ar.result().getFile());
      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });
      request.handler(response -> {
        if (response.statusCode() == 204) {
          doDeleteChunks(paths, handler);
        } else {
          handler.handle(Future.failedFuture(response.statusMessage()));
        }
      });
      request.end();
    });
  }
}
