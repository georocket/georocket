package io.georocket.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Test {@link MapUtils}
 * @author Michel Kraemer
 */
public class MapUtilsTest {
  /**
   * Merge a map into an empty map
   */
  @Test
  public void empty() {
    Map<String, String> m1 = new HashMap<>();
    Map<String, String> m2 = new HashMap<>();
    m2.put("a", "1");
    m2.put("b", "2");
    MapUtils.deepMerge(m1, m2);
    assertEquals(m2, m1);
  }
  
  /**
   * Overwrite an existing key
   */
  @Test
  public void existingKey() {
    Map<String, String> m1 = new HashMap<>();
    m1.put("a", "1");
    m1.put("b", "0");
    Map<String, String> m2 = new HashMap<>();
    m2.put("b", "2");
    m2.put("c", "3");
    Map<String, String> expected = new HashMap<>();
    expected.put("a", "1");
    expected.put("b", "2");
    expected.put("c", "3");
    MapUtils.deepMerge(m1, m2);
    assertEquals(expected, m1);
  }
  
  /**
   * Merge a nested map
   */
  @Test
  public void nestedMap() {
    Map<String, String> nested1 = new HashMap<>();
    nested1.put("a", "1");
    nested1.put("b", "0");
    Map<String, Object> m1 = new HashMap<>();
    m1.put("nested", nested1);
    Map<String, String> nested2 = new HashMap<>();
    nested2.put("b", "2");
    nested2.put("c", "3");
    Map<String, Object> m2 = new HashMap<>();
    m2.put("nested", nested2);
    Map<String, String> nestedExpected = new HashMap<>();
    nestedExpected.put("a", "1");
    nestedExpected.put("b", "2");
    nestedExpected.put("c", "3");
    Map<String, Object> expected = new HashMap<>();
    expected.put("nested", nestedExpected);
    MapUtils.deepMerge(m1, m2);
    assertEquals(expected, m1);
  }
  
  /**
   * Merge a nested map with a list
   */
  @Test
  public void withList() {
    Map<String, Object> nested1 = new HashMap<>();
    nested1.put("a", "1");
    nested1.put("b", "0");
    nested1.put("d", new ArrayList<String>(Arrays.asList("1")));
    Map<String, Object> m1 = new HashMap<>();
    m1.put("nested", nested1);
    Map<String, Object> nested2 = new HashMap<>();
    nested2.put("b", "2");
    nested2.put("c", "3");
    nested2.put("d", Arrays.asList("2"));
    Map<String, Object> m2 = new HashMap<>();
    m2.put("nested", nested2);
    Map<String, Object> nestedExpected = new HashMap<>();
    nestedExpected.put("a", "1");
    nestedExpected.put("b", "2");
    nestedExpected.put("c", "3");
    nestedExpected.put("d", Arrays.asList("1", "2"));
    Map<String, Object> expected = new HashMap<>();
    expected.put("nested", nestedExpected);
    MapUtils.deepMerge(m1, m2);
    assertEquals(expected, m1);
  }
  
  /**
   * Test if unmodifiable maps with the same content can be merged
   * without an error
   */
  @Test
  public void mergeUnmodifiableMaps() {
    Map<String, String> m1 = new HashMap<>();
    m1.put("a", "1");
    m1.put("b", "0");
    m1 = Collections.unmodifiableMap(m1);
    Map<String, String> m2 = new HashMap<>();
    m2.put("a", "1");
    m2.put("b", "0");
    m2 = Collections.unmodifiableMap(m2);
    Map<String, String> expected = new HashMap<>();
    expected.put("a", "1");
    expected.put("b", "0");
    MapUtils.deepMerge(m1, m2);
    assertEquals(expected, m1);
  }
  
  /**
   * Test if merging unmodifiable maps with different contents throws
   * an exception
   */
  @Test(expected = UnsupportedOperationException.class)
  public void mergeUnmodifiableMapsDifferent() {
    Map<String, String> m1 = new HashMap<>();
    m1.put("a", "1");
    m1.put("b", "0");
    m1 = Collections.unmodifiableMap(m1);
    Map<String, String> m2 = new HashMap<>();
    m2.put("a", "1");
    m2.put("b", "2");
    m2 = Collections.unmodifiableMap(m2);
    MapUtils.deepMerge(m1, m2);
  }
  
  /**
   * Test if unmodifiable collections with the same content can be merged
   * without an error
   */
  @Test
  public void mergeUnmodifiableCollections() {
    Map<String, Object> m1 = new HashMap<>();
    m1.put("a", "1");
    m1.put("b", Collections.unmodifiableList(Arrays.asList("1", "2")));
    Map<String, Object> m2 = new HashMap<>();
    m2.put("a", "1");
    m2.put("b", Collections.unmodifiableList(Arrays.asList("1", "2")));
    Map<String, Object> expected = new HashMap<>();
    expected.put("a", "1");
    expected.put("b", Arrays.asList("1", "2"));
    MapUtils.deepMerge(m1, m2);
    assertEquals(expected, m1);
  }
}
