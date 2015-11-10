package io.georocket.util;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.vertx.core.Vertx;

/**
 * A queue that calls an action after a given timeout or if a maximum number
 * of elements is reached. The action is supposed to remove items from the
 * queue.
 * @author Michel Kraemer
 * @param <T> the type of the items in the queue
 */
public class TimedActionQueue<T> {
  /**
   * The actual queue
   */
  private final Queue<T> queue = new ArrayDeque<T>();
  
  /**
   * The maximum number of items in the queue
   */
  private final int maxItems;
  
  /**
   * The time that must pass after the last call to {@link #offer(Object, BiConsumer)}
   * before the action will be called (milliseconds)
   */
  private final long delay;
  
  /**
   * The grace period between two calls to the action if the queue is still
   * not empty (milliseconds)
   */
  private final long grace;
  
  /**
   * The Vert.x instance
   */
  private final Vertx vertx;
  
  /**
   * The last time {@link #offer(Object, BiConsumer)} was called
   */
  private long lastOffer = 0;
  
  /**
   * The id of the currently active timer
   */
  private long timerId = -1;
  
  /**
   * Create a new queue
   * @param maxItems the maximum number of items in the queue
   * @param delay the time that must pass after the last call to
   * {@link #offer(Object, BiConsumer)} before the action will be called (milliseconds)
   * @param grace the grace period between two calls to the action if the
   * queue is still not empty (milliseconds)
   * @param vertx the Vert.x instance
   */
  public TimedActionQueue(int maxItems, long delay, long grace, Vertx vertx) {
    this.maxItems = maxItems;
    this.delay = delay;
    this.grace = grace;
    this.vertx = vertx;
  }
  
  /**
   * Adds a new item to the queue. Immediately calls the action if the
   * maximum number of items has been reached or starts a timer otherwise.
   * The action is supposed to remove items from the queue. In the best case
   * it makes the queue empty. If not the action will be called again after
   * a grace period.
   * @param item the item to add to the queue
   * @param action the action to call
   */
  public void offer(T item, Consumer<Queue<T>> action) {
    offer(item, (q, done) -> {
      action.accept(q);
      done.run();
    });
  }
  
  /**
   * Acts like {@link #offer(Object, Consumer)} but accepts an asynchronous
   * action. The action must call the callback that is passed to it to signal
   * its work is done.
   * @see #offer(Object, Consumer)
   * @param item the item to add to the queue
   * @param action the action to call
   */
  public void offer(T item, BiConsumer<Queue<T>, Runnable> action) {
    Runnable checkTimer = () -> {
      lastOffer = System.currentTimeMillis();
      if (!queue.isEmpty() && timerId < 0) {
        timerId = setTimer(delay, action);
      }
    };
    
    queue.offer(item);
    if (queue.size() >= maxItems) {
      action.accept(queue, checkTimer);
    } else {
      checkTimer.run();
    }
  }
  
  /**
   * Will be called when the timeout has elapsed
   * @param action the action to call to remove items from the queue
   */
  private void timerHandler(BiConsumer<Queue<T>, Runnable> action) {
    long elapsed = System.currentTimeMillis() - lastOffer;
    if (elapsed >= delay) {
      timerId = -1;
      action.accept(queue, () -> {
        if (!queue.isEmpty() && timerId < 0) {
          timerId = setTimer(grace, action);
        }
      });
    } else {
      timerId = setTimer(delay - elapsed, action);
    }
  }
  
  /**
   * Creates a new timer
   * @param delay the delay
   * @param action the action to call to remove items from the queue
   * @return the timer id
   */
  private long setTimer(long delay, BiConsumer<Queue<T>, Runnable> action) {
    return vertx.setTimer(delay, id -> timerHandler(action));
  }
}
