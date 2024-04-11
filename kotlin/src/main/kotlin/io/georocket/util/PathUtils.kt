package io.georocket.util

import java.net.URI
import java.net.URISyntaxException
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Utility functions to manipulate paths in a chunk store
 * @author Michel Kraemer
 */
object PathUtils {
  /**
   * Join all arguments and normalize the resulting chunk path
   * @param paths the arguments to join
   * @return a normalized chunk path
   */
  fun join(vararg paths: String?): String {
    val joined = Stream.of(*paths)
      .filter { p: String? -> p != null && p.isNotEmpty() }
      .collect(Collectors.joining("/"))
    return normalize(joined)
  }

  /**
   * Normalize a chunk path. Take care of "." and ".." as well as multiple slashes
   * @param path the chunk path to normalize
   * @return the normalized path
   */
  fun normalize(path: String): String {
    return try {
      var result = URI(path).normalize().toString()
      if (result.startsWith("//")) {
        result = result.substring(1)
      }
      result
    } catch (e: URISyntaxException) {
      throw IllegalArgumentException("Illegal path", e)
    }
  }

  /**
   * Removes a leading slash from the given chunk path (if there is any)
   * @param path the path
   * @return the path without leading slash
   */
  fun removeLeadingSlash(path: String?): String? {
    return if (path != null && path.isNotEmpty() && path[0] == '/') {
      path.substring(1)
    } else path
  }
}
