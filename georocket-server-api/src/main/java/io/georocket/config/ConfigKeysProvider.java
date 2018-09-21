package io.georocket.config;

import java.util.List;

/**
 * <p>Provides keys of configuration items.</p>
 *
 * <p>This interface can be implemented to register custom configuration items.
 * GeoRocket will collect a list of all configuration items at start-up and
 * initialize the global configuration (see {@link io.vertx.core.Context#config()})
 * with values from GeoRocket's configuration file or from the system environment.</p>
 *
 * <p>Implementations can be registered through the Java Service Provider
 * Interface (SPI) API.</p>
 *
 * @author Michel Kraemer
 * @since 1.4.0
 */
public interface ConfigKeysProvider {
  /**
   * Get a list of configuration item keys. For the sake of consistency, the
   * items should begin with the string {@code "georocket."}. For example, a
   * reasonable name would be {@code "georocket.myextension.mycategory.myitem"}.
   * @return the list of configuration item names
   */
  List<String> getConfigKeys();
}
