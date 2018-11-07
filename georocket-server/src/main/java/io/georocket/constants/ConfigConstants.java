package io.georocket.constants;

import io.georocket.config.ConfigKeysProvider;
import io.georocket.util.FilteredServiceLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Configuration constants
 * @author Michel Kraemer
 */
@SuppressWarnings("javadoc")
public final class ConfigConstants {
  public static final String HOME = "georocket.home";
  public static final String HOST = "georocket.host";
  public static final String PORT = "georocket.port";

  public static final String HTTP_COMPRESS = "georocket.http.compress";
  public static final String HTTP_SSL = "georocket.http.ssl";
  public static final String HTTP_CERT_PATH = "georocket.http.certPath";
  public static final String HTTP_KEY_PATH = "georocket.http.keyPath";
  public static final String HTTP_ALPN = "georocket.http.alpn";

  public static final String HTTP_CORS_ENABLE = "georocket.http.cors.enable";
  public static final String HTTP_CORS_ALLOW_ORIGIN = "georocket.http.cors.allowOrigin";
  public static final String HTTP_CORS_ALLOW_CREDENTIALS = "georocket.http.cors.allowCredentials";
  public static final String HTTP_CORS_ALLOW_HEADERS = "georocket.http.cors.allowHeaders";
  public static final String HTTP_CORS_ALLOW_METHODS = "georocket.http.cors.allowMethods";
  public static final String HTTP_CORS_EXPOSE_HEADERS = "georocket.http.cors.exposeHeaders";
  public static final String HTTP_CORS_MAX_AGE = "georocket.http.cors.maxAge";

  public static final String LOG_CONFIG = "georocket.logConfig";

  public static final String STORAGE_CLASS = "georocket.storage.class";
  public static final String STORAGE_H2_PATH = "georocket.storage.h2.path";
  public static final String STORAGE_H2_COMPRESS = "georocket.storage.h2.compress";
  public static final String STORAGE_H2_MAP_NAME = "georocket.storage.h2.mapName"; // undocumented
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
  public static final String INDEX_MAX_QUEUED_CHUNKS = "georocket.index.maxQueuedChunks";
  public static final String INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE = "georocket.index.indexableChunkCache.maxSize";
  public static final String INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS = "georocket.index.indexableChunkCache.maxTimeSeconds";
  public static final String INDEX_ELASTICSEARCH_EMBEDDED = "georocket.index.elasticsearch.embedded";
  public static final String INDEX_ELASTICSEARCH_HOST = "georocket.index.elasticsearch.host";
  public static final String INDEX_ELASTICSEARCH_PORT = "georocket.index.elasticsearch.port";
  public static final String INDEX_ELASTICSEARCH_HOSTS = "georocket.index.elasticsearch.hosts";
  public static final String INDEX_ELASTICSEARCH_AUTO_UPDATE_HOSTS_INTERVAL_SECONDS = "georocket.index.elasticsearch.autoUpdateHostsIntervalSeconds";
  public static final String INDEX_ELASTICSEARCH_COMPRESS_REQUEST_BODIES = "georocket.index.elasticsearch.compressRequestBodies";
  public static final String INDEX_ELASTICSEARCH_JAVA_OPTS = "georocket.index.elasticsearch.javaOpts";
  public static final String INDEX_ELASTICSEARCH_DOWNLOAD_URL = "georocket.index.elasticsearch.downloadUrl"; // undocumented
  public static final String INDEX_ELASTICSEARCH_INSTALL_PATH = "georocket.index.elasticsearch.installPath"; // undocumented
  public static final String INDEX_SPATIAL_PRECISION = "georocket.index.spatial.precision";

  public static final String QUERY_COMPILER_CLASS = "georocket.query.defaultQueryCompiler"; // undocumented
  public static final String QUERY_DEFAULT_CRS = "georocket.query.defaultCRS";

  public static final String TASKS_RETAIN_SECONDS = "georocket.tasks.retainSeconds";

  public static final String DEFAULT_HOST = "127.0.0.1";
  public static final int DEFAULT_PORT = 63020;

  public static final int DEFAULT_INDEX_MAX_BULK_SIZE = 200;
  public static final int DEFAULT_INDEX_MAX_PARALLEL_INSERTS = 5;
  public static final int DEFAULT_INDEX_MAX_QUEUED_CHUNKS = 10000;
  public static final long DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE = 1024L * 1024 * 64; // 64 MB
  public static final long DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS = 60;

  public static final long DEFAULT_TASKS_RETAIN_SECONDS = 60 * 2;

  private ConfigConstants() {
    // hidden constructor
  }

  /**
   * Get all configuration keys from this class and all extensions registered
   * through the Service Provider Interface API.
   * @see ConfigKeysProvider
   * @return the list of configuration keys
   */
  public static List<String> getConfigKeys() {
    List<String> r = getConfigKeys(ConfigConstants.class);

    FilteredServiceLoader<ConfigKeysProvider> loader =
        FilteredServiceLoader.load(ConfigKeysProvider.class);
    for (ConfigKeysProvider ccp : loader) {
      r.addAll(ccp.getConfigKeys());
    }

    return r;
  }

  /**
   * Get all configuration keys by enumerating over all string constants beginning
   * with the prefix {@code georocket} from a given class
   * @param cls the class to inspect
   * @return the list of configuration keys
   */
  public static List<String> getConfigKeys(Class<?> cls) {
    return Arrays.stream(cls.getFields())
      .map(f -> {
        try {
          return f.get(null);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Could not access config constant", e);
        }
      })
      .filter(s -> s instanceof String)
      .map(String.class::cast)
      .filter(s -> s.startsWith("georocket"))
      .collect(Collectors.toCollection(ArrayList::new));
  }
}
