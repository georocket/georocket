package io.georocket.util;

import java.util.function.Predicate;

/**
 * <p>A service filter that decides whether a service loaded through the
 * Service Provider Interface (SPI) should be used or skipped.</p>
 * <p>A new service filter can itself be provided through SPI. Service filters
 * are never filtered through other filters. Filters may be called in any
 * (random) order.</p>
 * @author Michel Kraemer
 * @since 1.2.0
 */
public interface ServiceFilter extends Predicate<Object> {
  /**
   * Filter a service instance
   * @param service the service instance
   * @return <code>true</code> if the service instance should be used,
   * <code>false</code> if it should be skipped (filtered out)
   */
  @Override
  boolean test(Object service);
}
