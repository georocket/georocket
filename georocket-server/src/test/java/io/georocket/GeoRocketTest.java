package io.georocket;

import com.google.common.collect.Sets;
import io.georocket.constants.ConfigConstants;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link GeoRocket}
 * @author Tim Hellhake
 */
public class GeoRocketTest {
  /**
   * Test if the configuration values are overwritten by environment values
   */
  @Test
  public void testOverwriteWithEnvironmentVariables() {
    final String PROP_KEY = ConfigConstants.LOG_CONFIG;
    final String ENV_KEY = PROP_KEY
      .replace(".", "_")
      .toUpperCase();
    final String VALUE = "test";
    JsonObject conf = new JsonObject();
    Map<String, String> env = new HashMap<String, String>();
    env.put(ENV_KEY, VALUE);
    GeoRocket.overwriteWithEnvironmentVariables(conf, env);
    assertEquals(Sets.newHashSet(PROP_KEY), conf.getMap().keySet());
    assertEquals(VALUE, conf.getString(PROP_KEY));
  }
}
