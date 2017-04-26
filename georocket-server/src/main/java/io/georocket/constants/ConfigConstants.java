package io.georocket.constants;

/**
 * Configuration constants
 * @author Michel Kraemer
 */
@SuppressWarnings("javadoc")
public final class ConfigConstants {
  public static final String HOME = "georocket.home";
  public static final String HOST = "georocket.host";
  public static final String PORT = "georocket.port";
  
  public static final String LOG_CONFIG = "georocket.logConfig";
  
  public static final String PAGINATION_ENABLED = "georocket.storage.pagination";
  
  public static final String STORAGE_CLASS = "georocket.storage.class";
  public static final String STORAGE_FILE_PATH = "georocket.storage.file.path";
  public static final String STORAGE_HDFS_DEFAULT_FS = "georocket.storage.hdfs.defaultFS";
  public static final String STORAGE_HDFS_PATH = "georocket.storage.hdfs.path";
  public static final String STORAGE_MONGODB_CONNECTION_STRING = "georocket.storage.mongodb.connectionString";
  public static final String STORAGE_MONGODB_DATABASE = "georocket.storage.mongodb.database";
  public static final String STORAGE_S3_ACCESS_KEY = "georocket.storage.s3.accessKey";
  public static final String STORAGE_S3_SECRET_KEY = "georocket.storage.s3.secretKey";
  public static final String STORAGE_S3_HOST = "georocket.storage.s3.host";
  public static final String STORAGE_S3_PORT = "georocket.storage.s3.port";
  public static final String STORAGE_S3_BUCKET = "georocket.storage.s3.bucket";
  public static final String STORAGE_S3_PATH_STYLE_ACCESS = "georocket.storage.s3.pathStyleAccess";
  public static final String STORAGE_S3_FORCE_SIGNATURE_V2 = "georocket.storage.s3.forceSignatureV2";
  public static final String STORAGE_S3_REQUEST_EXPIRY_SECONDS = "georocket.storage.s3.requestExpirySeconds";
  
  public static final String INDEX_MAX_BULK_SIZE = "georocket.index.maxBulkSize";
  public static final String INDEX_MAX_PARALLEL_INSERTS = "georocket.index.maxParallelInserts";
  public static final String INDEX_ELASTICSEARCH_EMBEDDED = "georocket.index.elasticsearch.embedded";
  public static final String INDEX_ELASTICSEARCH_HOST = "georocket.index.elasticsearch.host";
  public static final String INDEX_ELASTICSEARCH_PORT = "georocket.index.elasticsearch.port";
  public static final String INDEX_ELASTICSEARCH_DOWNLOAD_URL = "georocket.index.elasticsearch.downloadUrl";
  public static final String INDEX_ELASTICSEARCH_INSTALL_PATH = "georocket.index.elasticsearch.installPath";
  public static final String INDEX_SPATIAL_PRECISION = "georocket.index.spatial.precision";

  public static final String QUERY_COMPILER_CLASS = "georocket.query.defaultQueryCompiler";

  public static final String DEFAULT_HOST = "127.0.0.1";
  public static final int DEFAULT_PORT = 63020;
  
  public static final int DEFAULT_INDEX_MAX_BULK_SIZE = 200;
  public static final int DEFAULT_INDEX_MAX_PARALLEL_INSERTS = 5;
  public static final boolean DEFAULT_PAGINATION_ENABLED = false;
  
  
  
  private ConfigConstants() {
    // hidden constructor
  }
}
