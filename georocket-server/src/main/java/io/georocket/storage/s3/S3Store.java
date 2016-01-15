package io.georocket.storage.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;

import org.bson.types.ObjectId;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringInputStream;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
  private final String endpoint;
  private final String bucket;
  private final boolean pathStyleAccess;
  
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
    endpoint = config.getString(ConfigConstants.STORAGE_S3_ENDPOINT);
    bucket = config.getString(ConfigConstants.STORAGE_S3_BUCKET);
    pathStyleAccess = config.getBoolean(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, true);
  }
  
  private void ensureS3Client() {
    if (s3Client == null) {
      BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      s3Client = new AmazonS3Client(credentials);
      s3Client.setEndpoint(endpoint);
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
    
    vertx.executeBlocking(f -> {
      ensureS3Client();
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(chunk.length());
      try (InputStream is = new StringInputStream(chunk)) {
        s3Client.putObject(bucket, PathUtils.removeLeadingSlash(filename), is, om);
      } catch (IOException e) {
        f.fail(e);
        return;
      }
      f.complete(filename);
    }, handler);
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    vertx.<S3Object>executeBlocking(f -> {
      ensureS3Client();
      S3Object r = s3Client.getObject(bucket, PathUtils.normalize(path));
      f.complete(r);
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        S3Object r = ar.result();
        long size = r.getObjectMetadata().getContentLength();
        InputStream is = r.getObjectContent();
        handler.handle(Future.succeededFuture(new S3ChunkReadStream(is, size, vertx)));
      }
    });
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }
    
    String path = PathUtils.removeLeadingSlash(PathUtils.normalize(paths.poll()));
    vertx.executeBlocking(f -> {
      ensureS3Client();
      s3Client.deleteObject(bucket, path);
      f.complete();
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        doDeleteChunks(paths, handler);
      }
    });
  }
}
