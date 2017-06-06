package io.georocket;

import com.google.common.collect.Sets;
import io.georocket.constants.ConfigConstants;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Test {@link GeoRocket}
 * @author Tim Hellhake
 */
public class GeoRocketTest {
  @Test
  public void testOverwriteWithEnvironmentVariables() {
    final String PROP_KEY = ConfigConstants.LOG_CONFIG;
    final String ENV_KEY = PROP_KEY
      .replace(".", "_")
      .toUpperCase();
    final String VALUE = "test";
    JsonObject conf = new JsonObject();
    Map<String, String> env = new HashMap<String, String>() {{
      put(ENV_KEY, VALUE);
    }};
    GeoRocket.overwriteWithEnvironmentVariables(conf, env);
    Assert.assertEquals(Sets.newHashSet(PROP_KEY), conf.getMap().keySet());
    Assert.assertEquals(VALUE, conf.getString(PROP_KEY));
  }
}
