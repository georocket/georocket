package io.georocket;

/**
 * Configuration constants
 * @author Michel Kraemer
 */
@SuppressWarnings("javadoc")
public final class ConfigConstants {
  public static final String HOST = "georocket.host";
  public static final String PORT = "georocket.port";
  
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 63074;
  
  private ConfigConstants() {
    // hidden constructor
  }
}
