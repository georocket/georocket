package io.georocket.tasks;

import io.georocket.constants.AddressConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * A verticle that tracks information about currently running tasks
 * @author Michel Kraemer
 */
public class TaskVerticle extends AbstractVerticle {
  private Map<String, Map<Class<? extends Task>, Task>> tasks = new HashMap<>();

  @Override
  public void start() {
    vertx.eventBus().consumer(AddressConstants.TASK_INC, this::onInc);
  }

  /**
   * Handle requests to increment the values of a task
   * @param msg the request
   */
  private void onInc(Message<JsonObject> msg) {
    JsonObject body = msg.body();
    if (body == null) {
      // ignore
      return;
    }

    Task t = body.mapTo(Task.class);

    Map<Class<? extends Task>, Task> m = tasks.computeIfAbsent(
        t.getCorrelationId(), k -> new HashMap<>());

    Task existingTask = m.get(t.getClass());
    if (existingTask != null) {
      existingTask.inc(t);
    } else {
      m.put(t.getClass(), t);
    }
  }
}
