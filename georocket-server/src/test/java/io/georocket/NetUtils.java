package io.georocket;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Helper class for server tests.
 * @author Benedikt Hiemenz
 */
public class NetUtils {

  /**
   * Find a free socket port.
   * @return the number of the free port
   */
  public static int findPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Could not find a free port");
    }
  }
}
