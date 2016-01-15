package io.georocket.constants;

/**
 * Configuration constants
 * @author Michel Kraemer
 */
@SuppressWarnings("javadoc")
public final class ConfigConstants {
  public static final String HOME = "georocket.home";
  public static final String PORT = "georocket.port";
  
  public static final String STORAGE_CLASS = "georocket.storage.class";
  public static final String STORAGE_FILE_PATH = "georocket.storage.file.path";
  public static final String STORAGE_S3_ACCESS_KEY = "georocket.storage.s3.accessKey";
  public static final String STORAGE_S3_SECRET_KEY = "georocket.storage.s3.secretKey";
  public static final String STORAGE_S3_ENDPOINT = "georocket.storage.s3.endpoint";
  public static final String STORAGE_S3_BUCKET = "georocket.storage.s3.bucket";
  public static final String STORAGE_S3_PATH_STYLE_ACCESS = "georocket.storage.s3.pathStyleAccess";
  
  public static final int DEFAULT_PORT = 63020;
  
  private ConfigConstants() {
    // hidden constructor
  }
}
