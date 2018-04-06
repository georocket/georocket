package io.georocket.util;

import org.jooq.lambda.Seq;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Predicate;

/**
 * Wraps around {@link ServiceLoader} and lets {@link ServiceFilter} instances
 * decide whether a service instance should be used or skipped (filtered out).
 * {@link ServiceFilter}s will never be filtered by other {@link ServiceFilter}s.
 * {@link ServiceFilter}s may be called in any (random) order.
 * @param <S> the type of the services to load
 * @author Michel Kraemer
 */
public class FilteredServiceLoader<S> implements Iterable<S> {
  private final ServiceLoader<S> loader;
  private final Predicate<Object> filter;

  /**
   * Wrap around a {@link ServiceLoader}
   * @param loader the loader to wrap around
   */
  private FilteredServiceLoader(ServiceLoader<S> loader) {
    this.loader = loader;

    // load all service filters and combine to a single predicate
    Predicate<Object> r = null;
    for (ServiceFilter f : ServiceLoader.load(ServiceFilter.class)) {
      if (r == null) {
        r = f;
      } else {
        r = r.and(f);
      }
    }
    if (r == null) {
      r = service -> true;
    }
    this.filter = r;
  }

  /**
   * Creates a new service loader for the given service type.
   * @see ServiceLoader#load(Class)
   * @param cls an interface or an abstract class representing the service
   * @param <S> the class of the service type
   * @return a new service loader
   */
  public static <S> FilteredServiceLoader<S> load(Class<S> cls) {
    return new FilteredServiceLoader<>(ServiceLoader.load(cls));
  }

  /**
   * Returns an iterator to lazily load and instantiate the available
   * providers of this loader's service. Services will be filtered through
   * all available {@link ServiceFilter}s before they are returned.
   * @see ServiceLoader#iterator()
   * @return the iterator
   */
  @Override
  public Iterator<S> iterator() {
    return Seq.seq(loader)
      .filter(filter)
      .iterator();
  }
}
