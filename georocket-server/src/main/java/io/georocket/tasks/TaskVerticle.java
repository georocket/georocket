package io.georocket.tasks;

import io.georocket.constants.AddressConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A verticle that tracks information about currently running tasks
 * @author Michel Kraemer
 */
public class TaskVerticle extends AbstractVerticle {
  private Map<String, Map<Class<? extends Task>, Task>> tasks = new LinkedHashMap<>();

  @Override
  public void start() {
    vertx.eventBus().consumer(AddressConstants.TASK_GET_ALL, this::onGetAll);
    vertx.eventBus().consumer(AddressConstants.TASK_GET_BY_CORRELATION_ID,
        this::onGetByCorrelationId);
    vertx.eventBus().consumer(AddressConstants.TASK_INC, this::onInc);
  }

  /**
   * Handle a request to get all tasks
   * @param msg the request
   */
  private void onGetAll(Message<Void> msg) {
    JsonObject result = new JsonObject();
    tasks.forEach((c, m) -> result.put(c, makeResponse(m)));
    msg.reply(result);
  }

  /**
   * Handle a request to get all tasks of a given correlation ID
   * @param msg the request
   */
  private void onGetByCorrelationId(Message<String> msg) {
    String correlationId = msg.body();
    if (correlationId == null) {
      msg.fail(400, "Correlation ID expected");
      return;
    }

    Map<Class<? extends Task>, Task> m = tasks.get(correlationId);
    if (m == null) {
      msg.fail(404, "Unknown correlation ID");
      return;
    }

    JsonObject result = new JsonObject();
    result.put(correlationId, makeResponse(m));
    msg.reply(result);
  }

  /**
   * Make a response containing information about single correlation ID
   * @param m the information about the correlation ID
   * @return the response
   */
  private JsonArray makeResponse(Map<Class<? extends Task>, Task> m) {
    JsonArray arr = new JsonArray();
    m.forEach((cls, t) -> {
      JsonObject o = JsonObject.mapFrom(t);
      o.remove("correlationId");
      arr.add(o);
    });
    return arr;
  }

  /**
   * Handle a request to increment the values of a task
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
        t.getCorrelationId(), k -> new LinkedHashMap<>());

    Task existingTask = m.get(t.getClass());
    if (existingTask != null) {
      existingTask.inc(t);
    } else {
      m.put(t.getClass(), t);
    }
  }
}
