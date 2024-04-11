package io.georocket.constants

import java.lang.reflect.Field
import java.util.*
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * Configuration constants
 * @author Michel Kraemer
 */
object ConfigConstants {
  const val HOME = "georocket.home"
  const val HOST = "georocket.host"
  const val PORT = "georocket.port"
  const val HTTP_COMPRESS = "georocket.http.compress"
  const val HTTP_SSL = "georocket.http.ssl"
  const val HTTP_CERT_PATH = "georocket.http.certPath"
  const val HTTP_KEY_PATH = "georocket.http.keyPath"
  const val HTTP_ALPN = "georocket.http.alpn"
  const val HTTP_POST_MAX_SIZE = "georocket.http.postMaxSize"
  const val HTTP_CORS_ENABLE = "georocket.http.cors.enable"
  const val HTTP_CORS_ALLOW_ORIGIN = "georocket.http.cors.allowOrigin"
  const val HTTP_CORS_ALLOW_CREDENTIALS = "georocket.http.cors.allowCredentials"
  const val HTTP_CORS_ALLOW_HEADERS = "georocket.http.cors.allowHeaders"
  const val HTTP_CORS_ALLOW_METHODS = "georocket.http.cors.allowMethods"
  const val HTTP_CORS_EXPOSE_HEADERS = "georocket.http.cors.exposeHeaders"
  const val HTTP_CORS_MAX_AGE = "georocket.http.cors.maxAge"
  const val LOG_CONFIG = "georocket.logConfig"
  const val STORAGE_DRIVER = "georocket.storage.driver"
  const val STORAGE_H2_PATH = "georocket.storage.h2.path"
  const val STORAGE_H2_COMPRESS = "georocket.storage.h2.compress"
  const val STORAGE_H2_MAP_NAME = "georocket.storage.h2.mapName" // undocumented
  const val STORAGE_FILE_PATH = "georocket.storage.file.path"
  const val STORAGE_MONGODB_CONNECTION_STRING = "georocket.storage.mongodb.connectionString"
  const val STORAGE_POSTGRESQL_URL = "georocket.storage.postgresql.url"
  const val STORAGE_POSTGRESQL_USERNAME = "georocket.storage.postgresql.username"
  const val STORAGE_POSTGRESQL_PASSWORD = "georocket.storage.postgresql.password"
  const val STORAGE_S3_ACCESS_KEY = "georocket.storage.s3.accessKey"
  const val STORAGE_S3_SECRET_KEY = "georocket.storage.s3.secretKey"
  const val STORAGE_S3_ENDPOINT = "georocket.storage.s3.endpoint"
  const val STORAGE_S3_REGION = "georocket.storage.s3.region"
  const val STORAGE_S3_BUCKET = "georocket.storage.s3.bucket"
  const val STORAGE_S3_PATH_STYLE_ACCESS = "georocket.storage.s3.pathStyleAccess"
  const val INDEX_DRIVER = "georocket.index.driver"
  const val INDEX_INDEXED_FIELDS = "georocket.index.indexedFields"
  const val INDEX_MAX_BULK_SIZE = "georocket.index.maxBulkSize"
  const val INDEX_MONGODB_CONNECTION_STRING = "georocket.index.mongodb.connectionString"
  const val INDEX_POSTGRESQL_URL = "georocket.index.postgresql.url"
  const val INDEX_POSTGRESQL_USERNAME = "georocket.index.postgresql.username"
  const val INDEX_POSTGRESQL_PASSWORD = "georocket.index.postgresql.password"
  const val EMBEDDED_MONGODB_STORAGE_PATH = "georocket.embeddedMongoDB.storagePath"
  const val QUERY_DEFAULT_CRS = "georocket.query.defaultCRS"
  const val TASKS_RETAIN_SECONDS = "georocket.tasks.retainSeconds"
  const val LOGS_LEVEL = "georocket.logs.level"
  const val LOGS_ENABLED = "georocket.logs.enabled"
  const val LOGS_LOGFILE = "georocket.logs.logFile"
  const val LOGS_DAILYROLLOVER_ENABLED = "georocket.logs.dailyRollover.enabled"
  const val LOGS_DAILYROLLOVER_MAXDAYS = "georocket.logs.dailyRollover.maxDays"
  const val LOGS_DAILYROLLOVER_MAXSIZE = "georocket.logs.dailyRollover.maxSize"
  const val DEFAULT_HOST = "127.0.0.1"
  const val DEFAULT_PORT = 63020
  const val DEFAULT_INDEX_MAX_BULK_SIZE = 200
  const val DEFAULT_INDEX_MONGODB_CONNECTION_STRING = ""
  const val DEFAULT_TASKS_RETAIN_SECONDS = (60 * 2).toLong()
  const val DEFAULT_LOGS_LEVEL = "INFO"

  /**
   * Get all configuration keys by enumerating over all string constants beginning
   * with the prefix `georocket`
   * @return the list of configuration keys
   */
  val configKeys: List<String>
    get() = Arrays.stream(ConfigConstants::class.java.fields)
      .map { f: Field ->
        try {
          return@map f[null]
        } catch (e: IllegalAccessException) {
          throw RuntimeException("Could not access config constant", e)
        }
      }
      .filter { s: Any? -> s is String }
      .map { obj: Any? -> String::class.java.cast(obj) }
      .filter { s: String -> s.startsWith("georocket") }
      .collect(
        Collectors.toCollection(
          Supplier { ArrayList() })
      )
}
