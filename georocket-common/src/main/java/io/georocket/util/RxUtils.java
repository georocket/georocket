package io.georocket.util;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;

import io.vertx.core.logging.Logger;
import rx.Observable;
import rx.functions.Func1;

/**
 * Utility functions for RxJava
 * @author Michel Kraemer
 */
public final class RxUtils {
  private RxUtils() {
    // hidden constructor
  }
  
  /**
   * <p>Create a function that can be passed to {@link rx.Observable#retryWhen(Func1)}
   * in order to create a stream that is retried multiple times before failing.</p>
   * <p>Example:</p>
   * <pre>
   * Observable.create(subscriber -&gt; {
   *    // retryWhen will re-subscribe in case of error so this code
   *    // will be executed 5 times and finally fail with the given exception
   *    Observable.&lt;String&gt;error(new IllegalStateException()).subscribe(subscriber);
   * }).retryWhen(RxUtils.makeRetry(5, 1000));
   * </pre>
   * @param retries the number of retries
   * @param interval the number of milliseconds to wait before a retry
   * @return a function that can be passed to {@link rx.Observable#retryWhen(Func1)}
   */
  public static Func1<Observable<? extends Throwable>, Observable<Long>> makeRetry(
      int retries, int interval) {
    return makeRetry(retries, interval, null);
  }
  
  /**
   * <p>Create a function that can be passed to {@link rx.Observable#retryWhen(Func1)}
   * in order to create a stream that is retried multiple times before failing.</p>
   * <p>Example:</p>
   * <pre>
   * Observable.create(subscriber -&gt; {
   *    // retryWhen will re-subscribe in case of error so this code
   *    // will be executed 5 times and finally fail with the given exception
   *    Observable.&lt;String&gt;error(new IllegalStateException()).subscribe(subscriber);
   * }).retryWhen(RxUtils.makeRetry(5, 1000, null));
   * </pre>
   * @param retries the number of retries
   * @param interval the number of milliseconds to wait before a retry
   * @param log optional logger used to record failed attempts (may be null)
   * @return a function that can be passed to {@link rx.Observable#retryWhen(Func1)}
   */
  public static Func1<Observable<? extends Throwable>, Observable<Long>> makeRetry(
      int retries, int interval, Logger log) {
    return attempts -> {
      return attempts.zipWith(Observable.range(1, retries + 1), (n, i) -> Pair.of(n, i)).flatMap(t -> {
        if (t.getValue() > retries) {
          return Observable.error(t.getKey());
        }
        if (log != null) {
          log.warn("Operation failed", t.getKey());
          log.info("Retry " + t.getValue() + " in " + interval + " milliseconds");
        }
        return Observable.timer(interval, TimeUnit.MILLISECONDS);
      });
    };
  }
}
