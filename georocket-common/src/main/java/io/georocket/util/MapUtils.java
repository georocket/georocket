package io.georocket.util;

import java.util.Collection;
import java.util.Map;

/**
 * Utility functions to manipulate maps
 * @author Michel Kraemer
 */
public class MapUtils {
  /**
   * Perform a deep merge of two maps. Recursively merge the second map into
   * the first one. Merge nested maps and lists. Overwrite existing keys.
   * @param <K> the type of the keys of the maps to merge
   * @param <V> the type of the values of the maps to merge
   * @param m1 the map to merge into
   * @param m2 the map to merge
   */
  @SuppressWarnings("unchecked")
  public static <K, V> void deepMerge(Map<K, V> m1, Map<? extends K, ? extends V> m2) {
    for (K k : m2.keySet()) {
      V v1 = m1.get(k);
      V v2 = m2.get(k);
      if (v1 instanceof Collection && v2 instanceof Collection) {
        ((Collection<Object>)v1).addAll((Collection<Object>)v2);
      } else if (v1 instanceof Map && v2 instanceof Map) {
        deepMerge((Map<Object, Object>)v1, (Map<Object, Object>)v2);
      } else {
        m1.put(k, v2);
      }
    }
  }
}
