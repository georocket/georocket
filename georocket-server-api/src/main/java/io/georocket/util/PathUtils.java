package io.georocket.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility functions to manipulate paths in a chunk store
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class PathUtils {
  /**
   * Join all arguments and normalize the resulting chunk path
   * @param paths the arguments to join
   * @return a normalized chunk path
   */
  public static String join(String... paths) {
    String joined = Stream.of(paths)
        .filter(p -> p != null && !p.isEmpty())
        .collect(Collectors.joining("/"));
    return normalize(joined);
  }
  
  /**
   * Normalize a chunk path. Take care of "." and ".." as well as multiple slashes
   * @param path the chunk path to normalize
   * @return the normalized path
   */
  public static String normalize(String path) {
    try {
      String result = new URI(path).normalize().toString();
      if (result.startsWith("//")) {
        result = result.substring(1);
      }
      return result;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Illegal path", e);
    }
  }
  
  /**
   * Check if a chunk path is absolute
   * @param path the chunk path
   * @return true if the path is absolute, false otherwise
   */
  public static boolean isAbsolute(String path) {
    return path != null && !path.isEmpty() && path.charAt(0) == '/';
  }
  
  /**
   * Removes a leading slash from the given chunk path (if there is any)
   * @param path the path
   * @return the path without leading slash
   */
  public static String removeLeadingSlash(String path) {
    if (path != null && !path.isEmpty() && path.charAt(0) == '/') {
      return path.substring(1);
    }
    return path;
  }

  /**
   * Ensures the given chunk path ends with a trailing slash
   * @param path the path
   * @return the path with a trailing slash
   */
  public static String addTrailingSlash(String path) {
    if (path == null) {
      return null;
    }
    if (!path.isEmpty() && path.charAt(path.length() - 1) == '/') {
      return path;
    }
    return path + "/";
  }
}
